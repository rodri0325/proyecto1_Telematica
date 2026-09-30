#define _POSIX_C_SOURCE 200809L
#include "protocol.h"
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

static int parse_positive_ll(const char *s, long long *v) {
    if (!s || !*s || !isdigit((unsigned char)*s)) return 0;
    for (const char *p=s; *p; p++) if (!isdigit((unsigned char)*p)) return 0;
    errno=0; char *end=NULL; long long x=strtoll(s, &end, 10);
    if (errno || !end || *end || x<0) return 0;
    *v=x; return 1;
}
int safe_id(const char *s, size_t max_length) {
    size_t n= s ? strlen(s) : 0;
    if (!n || n>max_length) return 0;
    for (size_t i=0; i<n; i++) {
        unsigned char c=(unsigned char)s[i];
        if (!(isalnum(c) || c=='_' || c=='-' || c=='.')) return 0;
    }
    return 1;
}
/* Validación UTF-8, incluyendo sobrecodificación, sustitutos y codepoints fuera de Unicode. */
static int valid_utf8(const unsigned char *s) {
    while(*s) {
        unsigned char a=*s++;
        if(a<0x80)continue;
        unsigned int cp;int more;
        if(a>=0xC2 && a<=0xDF){cp=a&0x1f;more=1;}
        else if(a>=0xE0 && a<=0xEF){cp=a&0x0f;more=2;}
        else if(a>=0xF0 && a<=0xF4){cp=a&0x07;more=3;}
        else return 0;
        while(more--) {
            if((*s&0xC0)!=0x80)return 0;
            cp=(cp<<6)|(*s++&0x3F);
        }
        if((a<=0xDF && cp<0x80) || (a<=0xEF && cp<0x800) ||
           (a>=0xF0 && cp<0x10000) || (cp>=0xD800 && cp<=0xDFFF) || cp>0x10FFFF)
            return 0;
    }
    return 1;
}
static int token_valid(const char *token) {
    if (!strcmp(token,"-")) return 1;
    if (strlen(token)!=32) return 0;
    for (int i=0;i<32;i++) if (!isxdigit((unsigned char)token[i])) return 0;
    return 1;
}
int smdp_parse(char *line, SmdpMessage *m) {
    if (!valid_utf8((const unsigned char *)line)) return -1;
    /* Separar exclusivamente los 6 primeros '|': payload puede contener ese caracter. */
    char *v[7], *p=line;
    for (int i=0;i<6;i++) {
        v[i]=p; char *sep=strchr(p,'|');
        if (!sep) return -1;
        *sep='\0'; p=sep+1;
    }
    v[6]=p;
    if (strcmp(v[0],"SMDP/1.0")) return -2;
    long long seq,ts;
    if (!safe_id(v[1],32) || !safe_id(v[2],63) ||
        !parse_positive_ll(v[3],&seq) || !parse_positive_ll(v[4],&ts) ||
        !token_valid(v[5]) || !*v[6]) return -1;
    m->type=v[1]; m->origin=v[2]; m->seq_text=v[3]; m->ts_text=v[4];
    m->token=v[5]; m->payload=v[6]; m->seq=seq; m->timestamp=ts;
    return 0;
}
static void ws(const char **p) {while (**p && isspace((unsigned char)**p)) (*p)++;}
static int add_byte(char *dst,size_t cap,size_t *n,unsigned char c) {
    if (*n+1>=cap) return 0;
    dst[(*n)++]=(char)c; dst[*n]=0; return 1;
}
static int parse_json_str(const char **p, char *out, size_t cap) {
    if (**p!='"') return 0;
    (*p)++; size_t n=0; out[0]=0;
    while (**p && **p!='"') {
        unsigned char c=(unsigned char)**p;
        if (c<32) return 0;
        if (c!='\\') {if (!add_byte(out,cap,&n,c)) return 0; (*p)++; continue;}
        (*p)++; c=(unsigned char)**p;
        if (!c) return 0;
        switch (c) {
            case '"': c='"';break; case '\\':c='\\';break; case '/':c='/';break;
            case 'b':c='\b';break; case 'f':c='\f';break;
            case 'n':c='\n';break; case 'r':c='\r';break; case 't':c='\t';break;
            case 'u': {
                unsigned int cp=0;
                for (int i=0;i<4;i++) {
                    (*p)++; char h=**p;
                    if (!isxdigit((unsigned char)h)) return 0;
                    cp=16*cp+(unsigned int)(isdigit((unsigned char)h)?h-'0':tolower((unsigned char)h)-'a'+10);
                }
                if (cp<32 || (cp>=0xD800 && cp<=0xDFFF)) return 0; /* sin controles ni sustitutos */
                if (cp<0x80) {if (!add_byte(out,cap,&n,cp))return 0;}
                else if (cp<0x800) {
                    if (!add_byte(out,cap,&n,0xC0|(cp>>6)) || !add_byte(out,cap,&n,0x80|(cp&63)))return 0;
                } else {
                    if (!add_byte(out,cap,&n,0xE0|(cp>>12)) || !add_byte(out,cap,&n,0x80|((cp>>6)&63)) ||
                        !add_byte(out,cap,&n,0x80|(cp&63)))return 0;
                }
                (*p)++;continue;
            }
            default:return 0;
        }
        if (c<32 || !add_byte(out,cap,&n,c)) return 0;
        (*p)++;
    }
    if (**p!='"')return 0;
    (*p)++;return 1;
}
int json_parse_object(const char *text, JsonObject *o) {
    memset(o,0,sizeof(*o)); const char *p=text; ws(&p);
    if (*p++!='{')return 0;
    ws(&p);if (*p=='}') {p++;ws(&p);return !*p;}
    while (*p) {
        if (o->n>=SMDP_MAX_FIELDS)return 0;
        JsonField f={0};
        if (!parse_json_str(&p,f.key,sizeof(f.key)))return 0;
        if (!safe_id(f.key,63))return 0;
        for (int i=0;i<o->n;i++) if (!strcmp(o->fields[i].key,f.key)) return 0;
        ws(&p);if (*p++!=':')return 0;ws(&p);
        if (*p=='"') {
            f.is_string=1;
            if (!parse_json_str(&p,f.value,sizeof(f.value))) return 0;
        } else {
            const char *start=p;
            if (*p=='-')p++;
            if (!isdigit((unsigned char)*p))return 0;
            if (*p=='0')p++;
            else while (isdigit((unsigned char)*p))p++;
            if (*p=='.') {p++;if (!isdigit((unsigned char)*p))return 0;
                while (isdigit((unsigned char)*p))p++;}
            if (*p=='e'||*p=='E'){p++;if (*p=='+'||*p=='-')p++;
                if (!isdigit((unsigned char)*p))return 0;
                while (isdigit((unsigned char)*p))p++;}
            size_t length=(size_t)(p-start);
            if (length>=sizeof(f.value))return 0;
            memcpy(f.value,start,length);f.value[length]=0;
        }
        o->fields[o->n++]=f;
        ws(&p);
        if (*p=='}'){p++;ws(&p);return !*p;}
        if (*p++!=',')return 0;
        ws(&p);
    }
    return 0;
}
static const JsonField *lookup(const JsonObject *o,const char *key) {
    for(int i=0;i<o->n;i++)if(!strcmp(o->fields[i].key,key))return &o->fields[i];
    return NULL;
}
int json_has(const JsonObject *o,const char *key) {return lookup(o,key)!=NULL;}
int json_string(const JsonObject *o,const char *key,char *out,size_t cap) {
    const JsonField *f=lookup(o,key);
    if (!f || !f->is_string || strlen(f->value)>=cap)return 0;
    strcpy(out,f->value);return 1;
}
int json_long(const JsonObject *o,const char *key,long *out) {
    const JsonField *f=lookup(o,key);
    if (!f || f->is_string || strchr(f->value,'.') || strchr(f->value,'e') || strchr(f->value,'E'))return 0;
    errno=0;char *end=NULL;long val=strtol(f->value,&end,10);
    if (errno || *end)return 0;
    *out=val;return 1;
}
int json_double(const JsonObject *o,const char *key,double *out) {
    const JsonField *f=lookup(o,key);
    if (!f || f->is_string)return 0;
    errno=0;char *end=NULL;double val=strtod(f->value,&end);
    if (errno || *end || !isfinite(val))return 0;
    *out=val;return 1;
}
int send_all(int fd,const char *text) {
    size_t length=strlen(text),sent=0;
    while(sent<length) {
        ssize_t n=send(fd,text+sent,length-sent,MSG_NOSIGNAL);
        if (n<0 && errno==EINTR)continue;
        if (n<=0)return 0;
        sent+=(size_t)n;
    }
    return 1;
}
void smdp_response(char *dst,size_t cap,const char *type,const char *seq,
                   const char *token,const char *json) {
    snprintf(dst,cap,"SMDP/1.0|%s|server|%s|%lld|%s|%s\n",type,seq,
             (long long)time(NULL),token,json);
}
