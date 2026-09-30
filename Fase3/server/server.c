#define _POSIX_C_SOURCE 200809L
/* Servidor central SMDP/1.0 - fase 3. Socket TCP y UDP: API Berkeley. */
#include "protocol.h"
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define MAX_NODES 64
#define MAX_HISTORY 5
#define MAX_EVENTS 32
#define RECENT_EVENTS 64
#define MAX_SESSIONS 64
#define UDP_WORKERS 3
#define UDP_QUEUE 128
#define SESSION_LIFETIME 1800

typedef struct {int cpu,battery;double temp;char state[24];long long ts;long long seq;} Sample;
typedef struct {long long seq,ts;char type[64];double value,threshold;} Event;
typedef struct {
    char id[64], token[33];int registered;
    Sample history[MAX_HISTORY];int h_next,h_count;long long last_status_seq;
    Event events[MAX_EVENTS];int e_next,e_count;
    long long recent_seq[RECENT_EVENTS];int recent_next,recent_count;
    long long ack_dropped_seq;int has_dropped_ack;
} Node;
typedef struct {char token[33],user[64],profile[8];time_t expires;} Session;
typedef struct {int fd;struct sockaddr_storage peer;socklen_t peerlen;} Connection;
typedef struct {char data[SMDP_MAX_FRAME+1];size_t length;struct sockaddr_storage peer;socklen_t peerlen;} Datagram;

static Node nodes[MAX_NODES];static int node_count=0;
static Session sessions[MAX_SESSIONS];
static pthread_mutex_t node_lock=PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t session_lock=PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_lock=PTHREAD_MUTEX_INITIALIZER;
static FILE *log_file=NULL;
static int udp_fd=-1,drop_first_ack=0;
static const char *identity_host="localhost",*identity_port="6001";
/* Cada hilo conserva la identidad de su petición: logs sin credenciales/tokens. */
static _Thread_local char current_identity[64]="-";
/* Una cola FIFO por worker. Afinidad por id de nodo preserva el orden por emisor
 * aunque diferentes nodos se procesen en paralelo. */
typedef struct {
    Datagram packets[UDP_QUEUE];
    int first,last,count;
    pthread_mutex_t mutex;
    pthread_cond_t non_empty;
} WorkerQueue;
static WorkerQueue queues[UDP_WORKERS];
static int worker_ids[UDP_WORKERS];

static int port_from_str(const char *s) {
    if (!s || !*s)return 0;
    for(const char *p=s;*p;p++)if(*p<'0'||*p>'9')return 0;
    long v=strtol(s,NULL,10);return (v>=1 && v<=65535)?(int)v:0;
}
static void peer_info(const struct sockaddr_storage *addr,char *ip,size_t cap,unsigned short *port) {
    void *source=NULL;*port=0;
    if (addr->ss_family==AF_INET){const struct sockaddr_in *a=(const struct sockaddr_in*)addr;
        source=(void*)&a->sin_addr;*port=ntohs(a->sin_port);
    }else if(addr->ss_family==AF_INET6){const struct sockaddr_in6 *a=(const struct sockaddr_in6*)addr;
        source=(void*)&a->sin6_addr;*port=ntohs(a->sin6_port);}
    if(!source || !inet_ntop(addr->ss_family,source,ip,(socklen_t)cap)) snprintf(ip,cap,"unknown");
}
/* Log seguro: nunca incluye contraseña, token ni payload JSON. */
static void log_line(const struct sockaddr_storage *peer,const char *transport,
                     const char *direction,const char *type,const char *seq,const char *result) {
    char ip[INET6_ADDRSTRLEN];unsigned short port;
    peer_info(peer,ip,sizeof(ip),&port);
    time_t now=time(NULL);struct tm utc;gmtime_r(&now,&utc);
    char stamp[32];strftime(stamp,sizeof(stamp),"%Y-%m-%dT%H:%M:%SZ",&utc);
    pthread_mutex_lock(&log_lock);
    fprintf(stdout,"%s transport=%s peer=%s:%hu id=%s direction=%s type=%s seq=%s result=%s\n",
            stamp,transport,ip,port,current_identity,direction,type,seq,result);
    fflush(stdout);
    if(log_file) {
        int written=fprintf(log_file,"%s transport=%s peer=%s:%hu id=%s direction=%s type=%s seq=%s result=%s\n",
                            stamp,transport,ip,port,current_identity,direction,type,seq,result);
        if(written<0 || fflush(log_file)==EOF) {
            fprintf(stderr,"ADVERTENCIA: escritura de archivo log falló. Se mantiene salida en consola.\n");
            fclose(log_file);log_file=NULL;
        }
    }
    pthread_mutex_unlock(&log_lock);
}
static int secure_token(char token[33]) {
    unsigned char raw[16];FILE *f=fopen("/dev/urandom","rb");
    if(!f)return 0;
    size_t got=fread(raw,1,sizeof(raw),f);fclose(f);
    if(got!=sizeof(raw))return 0;
    static const char hex[]="0123456789abcdef";
    for(int i=0;i<16;i++){token[i*2]=hex[raw[i]>>4];token[i*2+1]=hex[raw[i]&15];}
    token[32]=0;return 1;
}
static Node *node_by_id(const char *id) { /* llamada bajo node_lock */
    for(int i=0;i<node_count;i++)if(!strcmp(nodes[i].id,id))return &nodes[i];
    return NULL;
}
static Node *node_by_token(const char *token) {
    for(int i=0;i<node_count;i++)if(nodes[i].registered && !strcmp(nodes[i].token,token))return &nodes[i];
    return NULL;
}
static void tcp_reply(Connection *c,const char *type,const char *seq,const char *token,const char *payload) {
    char answer[SMDP_MAX_FRAME+1];
    smdp_response(answer,sizeof(answer),type,seq,token,payload);
    int ok=send_all(c->fd,answer);
    log_line(&c->peer,"TCP","out",type,seq,ok?"SENT":"SEND_FAIL");
}
static void tcp_error(Connection *c,const char *seq,const char *code) {
    char json[128];snprintf(json,sizeof(json),"{\"code\":\"%s\"}",code);
    tcp_reply(c,"ERROR",seq,"-",json);
}
static void udp_reply(const Datagram *d,const char *type,const char *seq,
                      const char *token,const char *payload) {
    char response[256];smdp_response(response,sizeof(response),type,seq,token,payload);
    ssize_t n=sendto(udp_fd,response,strlen(response),0,(const struct sockaddr*)&d->peer,d->peerlen);
    log_line(&d->peer,"UDP","out",type,seq,n==(ssize_t)strlen(response)?"SENT":"SEND_FAIL");
}
static void udp_error(const Datagram *d,const char *seq,const char *code) {
    char body[100];snprintf(body,sizeof(body),"{\"code\":\"%s\"}",code);
    udp_reply(d,"ERROR",seq,"-",body);
}
/* Servicio separado. No almacenar usuarios ni contraseñas en servidor principal. */
static int validate_with_identity(const char *user,const char *password,char profile[8]) {
    struct addrinfo hints={0},*results=NULL;
    hints.ai_family=AF_UNSPEC;hints.ai_socktype=SOCK_STREAM;
    int resolved=0;
    for(int attempt=0;attempt<3;attempt++) {
        int error=getaddrinfo(identity_host,identity_port,&hints,&results);
        if(!error){resolved=1;break;}
        fprintf(stderr,"Servicio identidad: resolución fallida (%s), intento %d/3\n",
                gai_strerror(error),attempt+1);
        if(attempt<2)sleep((unsigned int)(1U<<attempt));
    }
    if(!resolved)return 0;
    int result=0;
    for(struct addrinfo *a=results;a;a=a->ai_next) {
        int fd=socket(a->ai_family,a->ai_socktype,a->ai_protocol);
        if(fd<0)continue;
        struct timeval tv={.tv_sec=3,.tv_usec=0};
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
        if(connect(fd,a->ai_addr,a->ai_addrlen)==0) {
            char request[180];snprintf(request,sizeof(request),"IDENT/1|%s|%s\n",user,password);
            if(send_all(fd,request)) {
                char buf[40]={0};int used=0;
                while(used<(int)sizeof(buf)-1){ssize_t n=recv(fd,buf+used,1,0);
                    if(n!=1)break;
                    used+=(int)n;
                    if(buf[used-1]=='\n')break;
                }
                if(!strcmp(buf,"OK|ADMIN\n")){strcpy(profile,"ADMIN");result=1;}
                else if(!strcmp(buf,"OK|VISOR\n")){strcpy(profile,"VISOR");result=1;}
            }
        }
        close(fd);if(result)break;
    }
    freeaddrinfo(results);return result;
}
static int create_session(const char *user,const char *profile,char token[33]) {
    pthread_mutex_lock(&session_lock);
    time_t now=time(NULL);int slot=-1;
    for(int i=0;i<MAX_SESSIONS;i++) {
        if(sessions[i].expires<=now){slot=i;break;}
    }
    int ok= slot>=0 && secure_token(token);
    if(ok) {
        Session *s=&sessions[slot];memset(s,0,sizeof(*s));
        strcpy(s->user,user);strcpy(s->profile,profile);strcpy(s->token,token);
        s->expires=now+SESSION_LIFETIME;
    }
    pthread_mutex_unlock(&session_lock);return ok;
}
static int session_profile(const char *token,char profile[8]) {
    int found=0;time_t now=time(NULL);
    pthread_mutex_lock(&session_lock);
    for(int i=0;i<MAX_SESSIONS;i++)if(sessions[i].expires>now && !strcmp(sessions[i].token,token)) {
        strcpy(profile,sessions[i].profile);found=1;break;
    }
    pthread_mutex_unlock(&session_lock);return found;
}
static void tcp_dispatch(Connection *c,char *line) {
    strcpy(current_identity,"-");
    SmdpMessage m={0};int status=smdp_parse(line,&m);
    if(status) {
        log_line(&c->peer,"TCP","in","INVALID","-",status==-2?"UNSUPPORTED_VERSION":"MALFORMED_MESSAGE");
        tcp_error(c,"0",status==-2?"UNSUPPORTED_VERSION":"MALFORMED_MESSAGE");return;
    }
    strcpy(current_identity,m.origin);
    log_line(&c->peer,"TCP","in",m.type,m.seq_text,"RECEIVED");
    JsonObject json;
    if(!json_parse_object(m.payload,&json)) {tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;}
    if(!strcmp(m.type,"REG")) {
        char device[64],location[64],token[33]={0};
        if(strcmp(m.token,"-") || !json_string(&json,"device_type",device,sizeof(device)) ||
           !json_string(&json,"location",location,sizeof(location)) ||
           !safe_id(device,63) || !safe_id(location,63)) {
            tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;
        }
        pthread_mutex_lock(&node_lock);
        Node *n=node_by_id(m.origin);
        if(!n && node_count<MAX_NODES){n=&nodes[node_count++];memset(n,0,sizeof(*n));strcpy(n->id,m.origin);}
        int ok=n && secure_token(token);
        if(ok) {strcpy(n->token,token);n->registered=1;
            /* El nodo puede reiniciar la secuencia al registrarse de nuevo. */
            n->last_status_seq=-1;n->recent_count=0;n->recent_next=0;n->has_dropped_ack=0;
        }
        pthread_mutex_unlock(&node_lock);
        if(ok)tcp_reply(c,"REG_OK",m.seq_text,token,"{\"status\":\"ok\"}");
        else tcp_error(c,m.seq_text,"INTERNAL_ERROR");
    } else if(!strcmp(m.type,"AUTH")) {
        char user[64],password[64],profile[8],token[33];
        if(strcmp(m.token,"-") || !json_string(&json,"user",user,sizeof(user)) ||
           !json_string(&json,"pass",password,sizeof(password)) || !safe_id(user,63) ||
           !safe_id(password,63) || strcmp(user,m.origin)) {
            tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;
        }
        if(!validate_with_identity(user,password,profile)) {
            tcp_reply(c,"AUTH_ERR",m.seq_text,"-","{\"code\":\"UNAUTHORIZED\"}");return;
        }
        if(!create_session(user,profile,token)){tcp_error(c,m.seq_text,"INTERNAL_ERROR");return;}
        char body[80];snprintf(body,sizeof(body),"{\"profile\":\"%s\"}",profile);
        tcp_reply(c,"AUTH_OK",m.seq_text,token,body);
    } else if(!strcmp(m.type,"QUERY")) {
        char profile[8];
        if(!session_profile(m.token,profile)) {tcp_error(c,m.seq_text,"UNAUTHORIZED");return;}
        char resource[32],id[64]="";long limit=MAX_HISTORY;
        if(!json_string(&json,"resource",resource,sizeof(resource)) ||
           !safe_id(resource,31)) {tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;}
        if(!strcmp(resource,"nodes") || !strcmp(resource,"diagnostic")) {
            if(strcmp(profile,"ADMIN")){tcp_error(c,m.seq_text,"UNAUTHORIZED");return;}
            long offset=0;
            if((json_has(&json,"limit") && !json_long(&json,"limit",&limit)) ||
               (json_has(&json,"offset") && !json_long(&json,"offset",&offset)) ||
               limit<1 || limit>10 || offset<0 || offset>MAX_NODES) {
                tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;
            }
            char body[3600];size_t pos=0;
            pthread_mutex_lock(&node_lock);
            pos+=(size_t)snprintf(body,sizeof(body),"{\"nodes\":[");
            int shown=0;
            for(int i=(int)offset;i<node_count && shown<(int)limit;i++){
                /* Paginación para que la respuesta TCP no exceda 4096 bytes. */
                int n=snprintf(body+pos,sizeof(body)-pos,"%s{\"id\":\"%s\",\"samples\":%d,\"events\":%d}",
                    shown?",":"",nodes[i].id,nodes[i].h_count,nodes[i].e_count);
                if(n<0 || (size_t)n>=sizeof(body)-pos){pthread_mutex_unlock(&node_lock);
                    tcp_error(c,m.seq_text,"INTERNAL_ERROR");return;}
                pos+=(size_t)n;shown++;
            }
            int total=node_count;
            pthread_mutex_unlock(&node_lock);
            snprintf(body+pos,sizeof(body)-pos,"],\"offset\":%ld,\"shown\":%d,\"total\":%d}",offset,shown,total);
            tcp_reply(c,"QUERY_RESP",m.seq_text,"-",body);return;
        }
        if(strcmp(resource,"status") && strcmp(resource,"history") && strcmp(resource,"events")) {
            tcp_error(c,m.seq_text,"NOT_FOUND");return;
        }
        if(!json_string(&json,"node",id,sizeof(id)) || !safe_id(id,63) ||
           (json_has(&json,"limit") && !json_long(&json,"limit",&limit))) {
            tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;
        }
        if(limit<1 || limit>10) {tcp_error(c,m.seq_text,"MALFORMED_MESSAGE");return;}
        char body[3600];size_t pos=0;
        pthread_mutex_lock(&node_lock);
        Node *node=node_by_id(id);
        if(!node){pthread_mutex_unlock(&node_lock);tcp_error(c,m.seq_text,"NOT_FOUND");return;}
        if(!strcmp(resource,"status")){
            if(node->h_count){int idx=(node->h_next+MAX_HISTORY-1)%MAX_HISTORY;Sample *s=&node->history[idx];
                snprintf(body,sizeof(body),"{\"node\":\"%s\",\"cpu\":%d,\"temp\":%.2f,\"battery\":%d,\"state\":\"%s\",\"ts\":%lld}",
                    id,s->cpu,s->temp,s->battery,s->state,s->ts);
            }else snprintf(body,sizeof(body),"{\"node\":\"%s\",\"state\":\"sin_datos\"}",id);
        } else if(!strcmp(resource,"history")) {
            int count=node->h_count<(int)limit?node->h_count:(int)limit;
            pos+=(size_t)snprintf(body,sizeof(body),"{\"node\":\"%s\",\"history\":[",id);
            for(int i=0;i<count;i++) {
                int idx=(node->h_next+MAX_HISTORY-count+i)%MAX_HISTORY;Sample *s=&node->history[idx];
                int n=snprintf(body+pos,sizeof(body)-pos,"%s{\"cpu\":%d,\"temp\":%.2f,\"battery\":%d,\"state\":\"%s\",\"ts\":%lld,\"seq\":%lld}",
                    i?",":"",s->cpu,s->temp,s->battery,s->state,s->ts,s->seq);
                if(n<0 || (size_t)n>=sizeof(body)-pos){pthread_mutex_unlock(&node_lock);
                    tcp_error(c,m.seq_text,"INTERNAL_ERROR");return;}
                pos+=(size_t)n;
            }
            snprintf(body+pos,sizeof(body)-pos,"]}");
        } else {
            int count=node->e_count<(int)limit?node->e_count:(int)limit;
            pos+=(size_t)snprintf(body,sizeof(body),"{\"node\":\"%s\",\"events\":[",id);
            for(int i=0;i<count;i++) {
                int idx=(node->e_next+MAX_EVENTS-count+i)%MAX_EVENTS;Event *e=&node->events[idx];
                int n=snprintf(body+pos,sizeof(body)-pos,"%s{\"event\":\"%s\",\"value\":%.2f,\"threshold\":%.2f,\"ts\":%lld,\"seq\":%lld}",
                    i?",":"",e->type,e->value,e->threshold,e->ts,e->seq);
                if(n<0 || (size_t)n>=sizeof(body)-pos){pthread_mutex_unlock(&node_lock);
                    tcp_error(c,m.seq_text,"INTERNAL_ERROR");return;}
                pos+=(size_t)n;
            }
            snprintf(body+pos,sizeof(body)-pos,"]}");
        }
        pthread_mutex_unlock(&node_lock);
        tcp_reply(c,"QUERY_RESP",m.seq_text,"-",body);
    } else tcp_error(c,m.seq_text,"UNKNOWN_TYPE");
}
/* TCP es un flujo: acumular hasta LF, manejar varias lineas por recv y recuperar overflow. */
static void *tcp_worker(void *arg) {
    Connection *c=(Connection*)arg;
    struct timeval tv={.tv_sec=30,.tv_usec=0};
    setsockopt(c->fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    char line[SMDP_MAX_FRAME+1];size_t used=0;int discard=0;
    for(;;){char chunk[1024];ssize_t n=recv(c->fd,chunk,sizeof(chunk),0);
        if(n<0 && errno==EINTR)continue;
        if(n<=0)break;
        for(ssize_t i=0;i<n;i++) {
            if(discard) {if(chunk[i]=='\n'){discard=0;used=0;}continue;}
            if(used>=SMDP_MAX_FRAME){tcp_error(c,"0","MALFORMED_MESSAGE");discard=chunk[i]!='\n';used=0;continue;}
            line[used++]=chunk[i];
            if(chunk[i]=='\n') {
                line[--used]=0;
                if(memchr(line,'\0',used))tcp_error(c,"0","MALFORMED_MESSAGE");
                else tcp_dispatch(c,line);
                used=0;
            }
        }
    }
    log_line(&c->peer,"TCP","in","DISCONNECT","-","CLOSED");
    close(c->fd);free(c);return NULL;
}
static void udp_dispatch(Datagram *d) {
    strcpy(current_identity,"-");
    /* Un datagrama equivale exactamente a un mensaje terminado en LF. */
    if(!d->length || d->length>SMDP_MAX_FRAME || d->data[d->length-1]!='\n' ||
       memchr(d->data,'\n',d->length-1) ||
       memchr(d->data,'\0',d->length)) {
        log_line(&d->peer,"UDP","in","INVALID","-","MALFORMED_MESSAGE");
        udp_error(d,"0","MALFORMED_MESSAGE");return;
    }
    d->data[d->length-1]=0;
    SmdpMessage m={0};int parsed=smdp_parse(d->data,&m);
    if(parsed) {
        log_line(&d->peer,"UDP","in","INVALID","-",parsed==-2?"UNSUPPORTED_VERSION":"MALFORMED_MESSAGE");
        udp_error(d,"0",parsed==-2?"UNSUPPORTED_VERSION":"MALFORMED_MESSAGE");return;
    }
    strcpy(current_identity,m.origin);
    log_line(&d->peer,"UDP","in",m.type,m.seq_text,"RECEIVED");
    if(strcmp(m.type,"STATUS") && strcmp(m.type,"EVENT")){
        udp_error(d,m.seq_text,"UNKNOWN_TYPE");return;
    }
    JsonObject json;
    if(!json_parse_object(m.payload,&json)) {udp_error(d,m.seq_text,"MALFORMED_MESSAGE");return;}
    if(!strcmp(m.type,"STATUS")) {
        long cpu,battery;double temp;char state[24];
        if(!json_long(&json,"cpu",&cpu) || cpu<0 || cpu>100 ||
           !json_double(&json,"temp",&temp) || temp< -100 || temp>250 ||
           !json_long(&json,"battery",&battery) || battery<0 || battery>100 ||
           !json_string(&json,"state",state,sizeof(state)) || !safe_id(state,23)) {
            udp_error(d,m.seq_text,"MALFORMED_MESSAGE");return;
        }
        pthread_mutex_lock(&node_lock);
        Node *node=node_by_token(m.token);
        if(!node || strcmp(node->id,m.origin)) {
            pthread_mutex_unlock(&node_lock);udp_error(d,m.seq_text,"NODE_NOT_REGISTERED");return;
        }
        if((long long)m.seq<=node->last_status_seq) {
            pthread_mutex_unlock(&node_lock);
            log_line(&d->peer,"UDP","internal","STATUS",m.seq_text,"OLD_IGNORED");return;
        }
        Sample *s=&node->history[node->h_next];
        s->cpu=(int)cpu;s->battery=(int)battery;s->temp=temp;strcpy(s->state,state);
        s->ts=m.timestamp;s->seq=m.seq;
        node->h_next=(node->h_next+1)%MAX_HISTORY;
        if(node->h_count<MAX_HISTORY)node->h_count++;
        node->last_status_seq=m.seq;
        pthread_mutex_unlock(&node_lock);
        log_line(&d->peer,"UDP","internal","STATUS",m.seq_text,"STORED_NO_ACK");
    } else {
        char event_type[64];double value,threshold;
        if(!json_string(&json,"event",event_type,sizeof(event_type)) ||
           !safe_id(event_type,63) || !json_double(&json,"value",&value) ||
           !json_double(&json,"threshold",&threshold) ||
           value < -1000000000 || value > 1000000000 ||
           threshold < -1000000000 || threshold > 1000000000) {
            udp_error(d,m.seq_text,"MALFORMED_MESSAGE");return;
        }
        int duplicate=0,drop=0;
        pthread_mutex_lock(&node_lock);
        Node *node=node_by_token(m.token);
        if(!node || strcmp(node->id,m.origin)) {
            pthread_mutex_unlock(&node_lock);udp_error(d,m.seq_text,"NODE_NOT_REGISTERED");return;
        }
        for(int i=0;i<node->recent_count;i++)if(node->recent_seq[i]==m.seq){duplicate=1;break;}
        if(!duplicate) {
            /* Guardar ANTES de ACK: los reintentos no generan eventos adicionales. */
            Event *e=&node->events[node->e_next];e->seq=m.seq;e->ts=m.timestamp;
            strcpy(e->type,event_type);e->value=value;e->threshold=threshold;
            node->e_next=(node->e_next+1)%MAX_EVENTS;
            if(node->e_count<MAX_EVENTS)node->e_count++;
            node->recent_seq[node->recent_next]=m.seq;
            node->recent_next=(node->recent_next+1)%RECENT_EVENTS;
            if(node->recent_count<RECENT_EVENTS)node->recent_count++;
        }
        if(drop_first_ack && (!node->has_dropped_ack || node->ack_dropped_seq!=m.seq)) {
            /* Simular perdida del primer ACK exclusivamente por EVENT nuevo. */
            if(!duplicate){node->ack_dropped_seq=m.seq;node->has_dropped_ack=1;drop=1;}
        }
        pthread_mutex_unlock(&node_lock);
        if(drop){log_line(&d->peer,"UDP","internal","ACK",m.seq_text,"SIMULATED_LOSS");return;}
        udp_reply(d,"ACK",m.seq_text,m.token,duplicate?"{\"duplicate\":true}":"{\"status\":\"ok\"}");
    }
}
static int shard_for(const char *data,size_t length) {
    /* Encabezado mínimo versión|tipo|ORIGEN|. Los inválidos se procesan en worker 0. */
    const char *end=data+length,*p=data;
    for(int i=0;i<2;i++){
        p=memchr(p,'|',(size_t)(end-p));
        if(!p)return 0;
        p++;
    }
    const char *sep=memchr(p,'|',(size_t)(end-p));
    if(!sep || sep-p>63)return 0;
    unsigned int h=2166136261U;
    for(;p<sep;p++)h=(h^(unsigned char)*p)*16777619U;
    return (int)(h%UDP_WORKERS);
}
static void *udp_worker(void *argument) {
    int id=*(int*)argument;
    WorkerQueue *q=&queues[id];
    for(;;){
        pthread_mutex_lock(&q->mutex);
        while(!q->count)pthread_cond_wait(&q->non_empty,&q->mutex);
        Datagram packet=q->packets[q->first];q->first=(q->first+1)%UDP_QUEUE;q->count--;
        pthread_mutex_unlock(&q->mutex);
        udp_dispatch(&packet);
    }
    return NULL;
}
static void *udp_receiver(void *unused) {
    (void)unused;
    for(;;) {
        Datagram packet={0};char buffer[SMDP_MAX_FRAME+1];
        packet.peerlen=sizeof(packet.peer);
        ssize_t n=recvfrom(udp_fd,buffer,sizeof(buffer),0,
                           (struct sockaddr*)&packet.peer,&packet.peerlen);
        if(n<0){if(errno==EINTR)continue;perror("recvfrom UDP");continue;}
        packet.length=(size_t)n;memcpy(packet.data,buffer,(size_t)n);
        int shard=shard_for(packet.data,packet.length);
        WorkerQueue *q=&queues[shard];
        pthread_mutex_lock(&q->mutex);
        if(q->count==UDP_QUEUE){pthread_mutex_unlock(&q->mutex);
            log_line(&packet.peer,"UDP","in","QUEUE","-","FULL_DROPPED");continue;}
        q->packets[q->last]=packet;q->last=(q->last+1)%UDP_QUEUE;q->count++;
        pthread_cond_signal(&q->non_empty);
        pthread_mutex_unlock(&q->mutex);
    }
    return NULL;
}
int main(int argc,char **argv) {
    if(argc!=3 || !port_from_str(argc==3?argv[1]:"")){
        fprintf(stderr,"Uso: %s <puerto> <archivoDeLogs>\n",argv[0]);return 1;}
    signal(SIGPIPE,SIG_IGN);
    const char *v=getenv("SMDP_IDENTITY_HOST");if(v && *v)identity_host=v;
    v=getenv("SMDP_IDENTITY_PORT");if(v && *v && port_from_str(v))identity_port=v;
    v=getenv("SMDP_DROP_FIRST_ACK");drop_first_ack=(v && !strcmp(v,"1"));
    log_file=fopen(argv[2],"a");
    if(!log_file)fprintf(stderr,"ADVERTENCIA: archivo de log no disponible; se registra en consola.\n");
    int port=port_from_str(argv[1]);
    int tcp=socket(AF_INET,SOCK_STREAM,0);udp_fd=socket(AF_INET,SOCK_DGRAM,0);
    if(tcp<0 || udp_fd<0){perror("socket");return 1;}
    int opt=1;setsockopt(tcp,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    struct sockaddr_in addr={0};addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_ANY);addr.sin_port=htons((uint16_t)port);
    if(bind(tcp,(struct sockaddr*)&addr,sizeof(addr)) || listen(tcp,64) ||
       bind(udp_fd,(struct sockaddr*)&addr,sizeof(addr))) {perror("bind/listen");return 1;}
    pthread_t t;
    for(int i=0;i<UDP_WORKERS;i++){
        worker_ids[i]=i;
        pthread_mutex_init(&queues[i].mutex,NULL);
        pthread_cond_init(&queues[i].non_empty,NULL);
        if(pthread_create(&t,NULL,udp_worker,&worker_ids[i])) {
            perror("pthread_create udp worker");return 1;
        }
        pthread_detach(t);
    }
    if(pthread_create(&t,NULL,udp_receiver,NULL)){perror("pthread_create udp receiver");return 1;}
    pthread_detach(t);
    printf("Servidor SMDP/1.0 TCP+UDP puerto=%d identity=%s:%s workers=%d\n",
           port,identity_host,identity_port,UDP_WORKERS);fflush(stdout);
    for(;;) {
        Connection *c=calloc(1,sizeof(*c));if(!c){perror("calloc");sleep(1);continue;}
        c->peerlen=sizeof(c->peer);
        c->fd=accept(tcp,(struct sockaddr*)&c->peer,&c->peerlen);
        if(c->fd<0){free(c);if(errno==EINTR)continue;perror("accept");continue;}
        if(pthread_create(&t,NULL,tcp_worker,c)){
            perror("pthread_create tcp");close(c->fd);free(c);continue;}
        pthread_detach(t);
    }
}
