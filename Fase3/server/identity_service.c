#define _POSIX_C_SOURCE 200809L
/* Servicio de identidad separado para laboratorio. Cambiar cuentas de demo antes de publicar. */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {int fd;} Client;
static const struct {const char *user,*password,*profile;} users[]={
    {"juan","1234","ADMIN"},{"maria","1234","VISOR"}
};
static void *handle(void *arg) {
    Client *c=arg;char request[200];int pos=0;
    while(pos<(int)sizeof(request)-1){ssize_t n=recv(c->fd,request+pos,1,0);
        if(n!=1)break;
        if(request[pos++]=='\n')break;
    }
    request[pos]=0;
    /* Nunca registrar la petición: contiene la contraseña. */
    char *prefix=strtok(request,"|"),*user=strtok(NULL,"|"),*password=strtok(NULL,"\n");
    const char *answer="DENIED\n";
    if(prefix && user && password && !strcmp(prefix,"IDENT/1")) {
        for(size_t i=0;i<sizeof(users)/sizeof(users[0]);i++) {
            if(!strcmp(user,users[i].user) && !strcmp(password,users[i].password)) {
                answer=!strcmp(users[i].profile,"ADMIN")?"OK|ADMIN\n":"OK|VISOR\n";
                break;
            }
        }
    }
    send(c->fd,answer,strlen(answer),MSG_NOSIGNAL);close(c->fd);free(c);return NULL;
}
int main(int argc,char **argv){
    if(argc!=2){fprintf(stderr,"Uso: %s <puerto>\n",argv[0]);return 1;}
    char *end;long port=strtol(argv[1],&end,10);
    if(*end || port<1 || port>65535){fprintf(stderr,"Puerto invalido\n");return 1;}
    signal(SIGPIPE,SIG_IGN);int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0){perror("socket");return 1;}
    int opt=1;setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    /* Solo loopback por defecto: contraseñas en claro, EXCLUSIVAMENTE LABORATORIO. */
    struct sockaddr_in addr={0};addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);addr.sin_port=htons((unsigned short)port);
    if(bind(fd,(struct sockaddr*)&addr,sizeof(addr)) || listen(fd,32)){perror("bind/listen");return 1;}
    printf("Servicio identidad de laboratorio escuchando en localhost:%ld\n",port);fflush(stdout);
    for(;;){Client *c=malloc(sizeof(*c));if(!c)continue;
        c->fd=accept(fd,NULL,NULL);if(c->fd<0){free(c);if(errno==EINTR)continue;perror("accept");continue;}
        pthread_t t;if(pthread_create(&t,NULL,handle,c)){close(c->fd);free(c);continue;}
        pthread_detach(t);
    }
}
