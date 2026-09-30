#ifndef SMDP_PROTOCOL_H
#define SMDP_PROTOCOL_H
#include <stddef.h>

#define SMDP_MAX_FRAME 4096
#define SMDP_MAX_FIELDS 16

typedef struct {
    char *type, *origin, *seq_text, *ts_text, *token, *payload;
    long long seq, timestamp;
} SmdpMessage;

typedef struct {
    char key[64];
    char value[256];
    int is_string;
} JsonField;

typedef struct {
    JsonField fields[SMDP_MAX_FIELDS];
    int n;
} JsonObject;

/* 0=OK, -1=formato, -2=version incorrecta. Modifica la linea recibida. */
int smdp_parse(char *line, SmdpMessage *out);
int json_parse_object(const char *text, JsonObject *obj);
int json_has(const JsonObject *obj, const char *name);
int json_string(const JsonObject *obj, const char *name, char *out, size_t cap);
int json_long(const JsonObject *obj, const char *name, long *out);
int json_double(const JsonObject *obj, const char *name, double *out);
int safe_id(const char *value, size_t max_length);
int send_all(int fd, const char *text);
void smdp_response(char *dst, size_t cap, const char *type,
                   const char *seq, const char *token, const char *json);
#endif
