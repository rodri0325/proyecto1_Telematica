/* Servidor central del protocolo SMDP/1.0.
 * Uso: ./servidor <puerto> <archivo_de_logs> */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAX_NODOS      64
#define MAX_SESIONES   64
#define MAX_USUARIOS   8
#define MAX_HIST        5
#define BUF_TAM      4096
#define TOKEN_LEN      17   /* 16 caracteres hex + '\0' */

/* Estructuras de datos */

typedef struct {
    int    cpu;
    double temp;
    int    battery;
    char   state[16];
    long   timestamp;
} Muestra;

typedef struct {
    char    id[64];
    char    token[TOKEN_LEN];
    int     registrado;
    Muestra historial[MAX_HIST];
    int     hist_count;   /* cuantas muestras validas hay */
    int     hist_pos;     /* siguiente posicion a escribir (circular) */
    long    ultimo_event_seq; /* ultimo EVENT ya procesado, para deduplicar */
    int     tiene_evento_previo;
} Nodo;

typedef struct {
    char username[32];
    char password[32];
    char perfil[8]; /* "VISOR" o "ADMIN" */
} Usuario;

typedef struct {
    char token[TOKEN_LEN];
    char username[32];
    char perfil[8];
    int  activo;
} Sesion;

static Nodo    nodos[MAX_NODOS];
static int     num_nodos = 0;
static pthread_mutex_t lock_nodos = PTHREAD_MUTEX_INITIALIZER;

static Sesion  sesiones[MAX_SESIONES];
static int     num_sesiones = 0;
static pthread_mutex_t lock_sesiones = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t lock_log = PTHREAD_MUTEX_INITIALIZER;
static FILE *archivo_log = NULL;

/* Usuarios de prueba de esta fase. */
static Usuario usuarios[MAX_USUARIOS] = {
    {"juan",  "1234", "ADMIN"},
    {"maria", "1234", "VISOR"}
};
static int num_usuarios = 2;

static int udp_sock_global = -1;

/* Utilidades generales */

static void generar_token(char *out) {
    static const char hexd[] = "0123456789abcdef";
    for (int i = 0; i < TOKEN_LEN - 1; i++) {
        out[i] = hexd[rand() % 16];
    }
    out[TOKEN_LEN - 1] = '\0';
}

static void log_evento(const char *transporte, const char *ip, int puerto,
                        const char *tipo, const char *seq, const char *identidad,
                        const char *resultado) {
    time_t ahora = time(NULL);
    struct tm tm_utc;
    gmtime_r(&ahora, &tm_utc);
    char fecha[32];
    strftime(fecha, sizeof(fecha), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);

    pthread_mutex_lock(&lock_log);
    printf("%s transport=%s peer=%s:%d type=%s seq=%s id=%s result=%s\n",
           fecha, transporte, ip, puerto, tipo, seq, identidad, resultado);
    if (archivo_log) {
        fprintf(archivo_log,
                "%s transport=%s peer=%s:%d type=%s seq=%s id=%s result=%s\n",
                fecha, transporte, ip, puerto, tipo, seq, identidad, resultado);
        fflush(archivo_log);
    }
    pthread_mutex_unlock(&lock_log);
}

/* Extrae un entero del JSON recibido. */
static int json_extraer_int(const char *json, const char *clave, int *out) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\"", clave);
    const char *p = strstr(json, patron);
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p == ' ') p++;
    *out = atoi(p);
    return 1;
}

static int json_extraer_double(const char *json, const char *clave, double *out) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\"", clave);
    const char *p = strstr(json, patron);
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p == ' ') p++;
    *out = atof(p);
    return 1;
}

/* Extrae un valor de texto del JSON recibido. */
static int json_extraer_str(const char *json, const char *clave, char *out, size_t len) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\"", clave);
    const char *p = strstr(json, patron);
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p == ' ') p++;
    if (*p != '"') return 0;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i < len - 1) {
        out[i++] = *p++;
    }
    out[i] = '\0';
    return 1;
}

/* Divide el mensaje en seis campos y conserva el resto como payload. */
static int dividir_mensaje(char *linea, char *campos[7]) {
    int n = 0;
    char *inicio = linea;
    while (n < 6) {
        char *sep = strchr(inicio, '|');
        if (!sep) return -1; /* formato invalido: faltan campos */
        *sep = '\0';
        campos[n++] = inicio;
        inicio = sep + 1;
    }
    campos[n++] = inicio; /* payload: resto de la linea */
    return n;
}

/* ---------------------------------------------------------------------
 * Manejo de nodos y sesiones
 * --------------------------------------------------------------------- */

static Nodo *buscar_nodo_por_id(const char *id) {
    for (int i = 0; i < num_nodos; i++) {
        if (strcmp(nodos[i].id, id) == 0) return &nodos[i];
    }
    return NULL;
}

static Nodo *buscar_nodo_por_token(const char *token) {
    for (int i = 0; i < num_nodos; i++) {
        if (strcmp(nodos[i].token, token) == 0) return &nodos[i];
    }
    return NULL;
}

static Sesion *buscar_sesion(const char *token) {
    for (int i = 0; i < num_sesiones; i++) {
        if (sesiones[i].activo && strcmp(sesiones[i].token, token) == 0) {
            return &sesiones[i];
        }
    }
    return NULL;
}

static int validar_credenciales(const char *user, const char *pass, char *perfil_out) {
    for (int i = 0; i < num_usuarios; i++) {
        if (strcmp(usuarios[i].username, user) == 0 &&
            strcmp(usuarios[i].password, pass) == 0) {
            strcpy(perfil_out, usuarios[i].perfil);
            return 1;
        }
    }
    return 0;
}

static void agregar_muestra(Nodo *n, int cpu, double temp, int battery, const char *state) {
    Muestra *m = &n->historial[n->hist_pos];
    m->cpu = cpu;
    m->temp = temp;
    m->battery = battery;
    strncpy(m->state, state, sizeof(m->state) - 1);
    m->state[sizeof(m->state) - 1] = '\0';
    m->timestamp = (long) time(NULL);

    n->hist_pos = (n->hist_pos + 1) % MAX_HIST;
    if (n->hist_count < MAX_HIST) n->hist_count++;
}

/* Hilo TCP: atiende REG, AUTH y QUERY. */

typedef struct {
    int fd;
    struct sockaddr_in addr;
} ClienteTCP;

static void construir_respuesta(char *out, size_t len, const char *tipo,
                                 const char *seq, const char *token, const char *payload) {
    snprintf(out, len, "SMDP/1.0|%s|-|%s|%ld|%s|%s\n",
             tipo, seq, (long) time(NULL), token, payload);
}

static void *manejar_cliente_tcp(void *arg) {
    ClienteTCP *cli = (ClienteTCP *) arg;
    int fd = cli->fd;
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &cli->addr.sin_addr, ip, sizeof(ip));
    int puerto = ntohs(cli->addr.sin_port);

    char buffer[BUF_TAM + 1];
    int usados = 0;

    for (;;) {
        /* Leer una linea completa. */
        char *nl = memchr(buffer, '\n', usados);
        if (!nl) {
            ssize_t leidos = recv(fd, buffer + usados, sizeof(buffer) - 1 - usados, 0);
            if (leidos <= 0) break; /* conexion cerrada o error */
            usados += leidos;
            buffer[usados] = '\0';
            nl = memchr(buffer, '\n', usados);
            if (!nl) {
                if (usados >= (int) sizeof(buffer) - 1) {
                    /* mensaje demasiado grande sin LF: se descarta el buffer */
                    usados = 0;
                }
                continue;
            }
        }

        int largo_linea = (int) (nl - buffer);
        char linea[BUF_TAM + 1];
        memcpy(linea, buffer, largo_linea);
        linea[largo_linea] = '\0';

        /* Conservar los datos que siguen a la linea actual. */
        int resto = usados - (largo_linea + 1);
        if (resto > 0) memmove(buffer, nl + 1, resto);
        usados = resto > 0 ? resto : 0;

        char *campos[7];
        char linea_copia[BUF_TAM + 1];
        strcpy(linea_copia, linea);
        char respuesta[BUF_TAM + 1];

        if (dividir_mensaje(linea_copia, campos) < 0 || strcmp(campos[0], "SMDP/1.0") != 0) {
            construir_respuesta(respuesta, sizeof(respuesta), "ERROR", "0", "-",
                                 "{\"code\":\"MALFORMED_MESSAGE\"}");
            send(fd, respuesta, strlen(respuesta), 0);
            log_evento("TCP", ip, puerto, "?", "-", "-", "ERROR");
            continue;
        }

        const char *tipo = campos[1];
        const char *origen = campos[2];
        const char *seq = campos[3];
        const char *payload = campos[6];

        if (strcmp(tipo, "REG") == 0) {
            char device_type[32] = "-", location[32] = "-";
            json_extraer_str(payload, "device_type", device_type, sizeof(device_type));
            json_extraer_str(payload, "location", location, sizeof(location));

            pthread_mutex_lock(&lock_nodos);
            Nodo *n = buscar_nodo_por_id(origen);
            if (!n && num_nodos < MAX_NODOS) {
                n = &nodos[num_nodos++];
                memset(n, 0, sizeof(*n));
                strncpy(n->id, origen, sizeof(n->id) - 1);
            }
            if (n) {
                generar_token(n->token);
                n->registrado = 1;
                char cuerpo[128];
                snprintf(cuerpo, sizeof(cuerpo), "{\"status\":\"ok\"}");
                construir_respuesta(respuesta, sizeof(respuesta), "REG_OK", seq, n->token, cuerpo);
                send(fd, respuesta, strlen(respuesta), 0);
                log_evento("TCP", ip, puerto, "REG", seq, origen, "REG_OK");
            } else {
                construir_respuesta(respuesta, sizeof(respuesta), "ERROR", seq, "-",
                                     "{\"code\":\"INTERNAL_ERROR\"}");
                send(fd, respuesta, strlen(respuesta), 0);
                log_evento("TCP", ip, puerto, "REG", seq, origen, "ERROR");
            }
            pthread_mutex_unlock(&lock_nodos);

        } else if (strcmp(tipo, "AUTH") == 0) {
            char user[32] = "", pass[32] = "";
            json_extraer_str(payload, "user", user, sizeof(user));
            json_extraer_str(payload, "pass", pass, sizeof(pass));

            char perfil[8];
            if (validar_credenciales(user, pass, perfil)) {
                pthread_mutex_lock(&lock_sesiones);
                Sesion *s = (num_sesiones < MAX_SESIONES) ? &sesiones[num_sesiones++] : NULL;
                if (s) {
                    generar_token(s->token);
                    strncpy(s->username, user, sizeof(s->username) - 1);
                    strncpy(s->perfil, perfil, sizeof(s->perfil) - 1);
                    s->activo = 1;
                    char cuerpo[64];
                    snprintf(cuerpo, sizeof(cuerpo), "{\"profile\":\"%s\"}", perfil);
                    construir_respuesta(respuesta, sizeof(respuesta), "AUTH_OK", seq, s->token, cuerpo);
                    send(fd, respuesta, strlen(respuesta), 0);
                    log_evento("TCP", ip, puerto, "AUTH", seq, user, "AUTH_OK");
                } else {
                    construir_respuesta(respuesta, sizeof(respuesta), "ERROR", seq, "-",
                                         "{\"code\":\"INTERNAL_ERROR\"}");
                    send(fd, respuesta, strlen(respuesta), 0);
                }
                pthread_mutex_unlock(&lock_sesiones);
            } else {
                construir_respuesta(respuesta, sizeof(respuesta), "AUTH_ERR", seq, "-",
                                     "{\"code\":\"UNAUTHORIZED\"}");
                send(fd, respuesta, strlen(respuesta), 0);
                log_evento("TCP", ip, puerto, "AUTH", seq, user, "AUTH_ERR");
            }

        } else if (strcmp(tipo, "QUERY") == 0) {
            const char *token = campos[5];
            pthread_mutex_lock(&lock_sesiones);
            Sesion *s = buscar_sesion(token);
            pthread_mutex_unlock(&lock_sesiones);

            if (!s) {
                construir_respuesta(respuesta, sizeof(respuesta), "ERROR", seq, "-",
                                     "{\"code\":\"UNAUTHORIZED\"}");
                send(fd, respuesta, strlen(respuesta), 0);
                log_evento("TCP", ip, puerto, "QUERY", seq, origen, "UNAUTHORIZED");
            } else {
                char nodo_id[64] = "", recurso[16] = "status";
                int limite = MAX_HIST;
                json_extraer_str(payload, "node", nodo_id, sizeof(nodo_id));
                json_extraer_str(payload, "resource", recurso, sizeof(recurso));
                json_extraer_int(payload, "limit", &limite);

                pthread_mutex_lock(&lock_nodos);
                Nodo *n = buscar_nodo_por_id(nodo_id);
                if (!n) {
                    pthread_mutex_unlock(&lock_nodos);
                    construir_respuesta(respuesta, sizeof(respuesta), "ERROR", seq, s->token,
                                         "{\"code\":\"NOT_FOUND\"}");
                    send(fd, respuesta, strlen(respuesta), 0);
                    log_evento("TCP", ip, puerto, "QUERY", seq, s->username, "NOT_FOUND");
                } else {
                    char cuerpo[BUF_TAM] = "";
                    if (strcmp(recurso, "history") == 0) {
                        char lista[BUF_TAM - 64] = "";
                        int mostrar = n->hist_count < limite ? n->hist_count : limite;
                        for (int k = 0; k < mostrar; k++) {
                            int idx = (n->hist_pos - mostrar + k + MAX_HIST) % MAX_HIST;
                            Muestra *m = &n->historial[idx];
                            char item[128];
                            snprintf(item, sizeof(item),
                                     "%s{\"cpu\":%d,\"temp\":%.1f,\"battery\":%d,\"state\":\"%s\",\"ts\":%ld}",
                                     k > 0 ? "," : "", m->cpu, m->temp, m->battery, m->state, m->timestamp);
                            strncat(lista, item, sizeof(lista) - strlen(lista) - 1);
                        }
                        snprintf(cuerpo, sizeof(cuerpo), "{\"node\":\"%s\",\"history\":[%s]}", nodo_id, lista);
                    } else {
                        int idx = (n->hist_pos - 1 + MAX_HIST) % MAX_HIST;
                        if (n->hist_count > 0) {
                            Muestra *m = &n->historial[idx];
                            snprintf(cuerpo, sizeof(cuerpo),
                                     "{\"node\":\"%s\",\"cpu\":%d,\"temp\":%.1f,\"battery\":%d,\"state\":\"%s\"}",
                                     nodo_id, m->cpu, m->temp, m->battery, m->state);
                        } else {
                            snprintf(cuerpo, sizeof(cuerpo), "{\"node\":\"%s\",\"state\":\"sin_datos\"}", nodo_id);
                        }
                    }
                    pthread_mutex_unlock(&lock_nodos);
                    construir_respuesta(respuesta, sizeof(respuesta), "QUERY_RESP", seq, s->token, cuerpo);
                    send(fd, respuesta, strlen(respuesta), 0);
                    log_evento("TCP", ip, puerto, "QUERY", seq, s->username, "QUERY_RESP");
                }
            }

        } else {
            construir_respuesta(respuesta, sizeof(respuesta), "ERROR", seq, "-",
                                 "{\"code\":\"UNKNOWN_TYPE\"}");
            send(fd, respuesta, strlen(respuesta), 0);
            log_evento("TCP", ip, puerto, tipo, seq, origen, "UNKNOWN_TYPE");
        }
    }

    close(fd);
    free(cli);
    return NULL;
}

/* Hilo UDP: atiende STATUS y EVENT. */

static void *escuchar_udp(void *arg) {
    (void) arg;
    char buffer[BUF_TAM + 1];

    for (;;) {
        struct sockaddr_in origen_addr;
        socklen_t addr_len = sizeof(origen_addr);
        ssize_t leidos = recvfrom(udp_sock_global, buffer, sizeof(buffer) - 1, 0,
                                   (struct sockaddr *) &origen_addr, &addr_len);
        if (leidos <= 0) continue;
        buffer[leidos] = '\0';
        /* quitar el salto de linea si vino incluido */
        char *nl = strchr(buffer, '\n');
        if (nl) *nl = '\0';

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &origen_addr.sin_addr, ip, sizeof(ip));
        int puerto = ntohs(origen_addr.sin_port);

        char *campos[7];
        char copia[BUF_TAM + 1];
        strcpy(copia, buffer);

        if (dividir_mensaje(copia, campos) < 0 || strcmp(campos[0], "SMDP/1.0") != 0) {
            log_evento("UDP", ip, puerto, "?", "-", "-", "MALFORMED_MESSAGE");
            continue;
        }

        const char *tipo = campos[1];
        const char *origen_id = campos[2];
        const char *seq = campos[3];
        const char *token = campos[5];
        const char *payload = campos[6];

        pthread_mutex_lock(&lock_nodos);
        Nodo *n = buscar_nodo_por_token(token);
        pthread_mutex_unlock(&lock_nodos);

        if (!n || strcmp(n->id, origen_id) != 0) {
            log_evento("UDP", ip, puerto, tipo, seq, origen_id, "NODE_NOT_REGISTERED");
            continue; /* datagrama sin respuesta, como indica la especificacion */
        }

        if (strcmp(tipo, "STATUS") == 0) {
            int cpu = 0, battery = 0;
            double temp = 0.0;
            char estado[16] = "ok";
            json_extraer_int(payload, "cpu", &cpu);
            json_extraer_double(payload, "temp", &temp);
            json_extraer_int(payload, "battery", &battery);
            json_extraer_str(payload, "state", estado, sizeof(estado));

            pthread_mutex_lock(&lock_nodos);
            agregar_muestra(n, cpu, temp, battery, estado);
            pthread_mutex_unlock(&lock_nodos);

            log_evento("UDP", ip, puerto, "STATUS", seq, origen_id, "ACTUALIZADO");

        } else if (strcmp(tipo, "EVENT") == 0) {
            char resp[BUF_TAM + 1];
            long seq_num = atol(seq);

            pthread_mutex_lock(&lock_nodos);
            int ya_procesado = n->tiene_evento_previo && n->ultimo_event_seq == seq_num;
            if (!ya_procesado) {
                n->ultimo_event_seq = seq_num;
                n->tiene_evento_previo = 1;
            }
            pthread_mutex_unlock(&lock_nodos);

            construir_respuesta(resp, sizeof(resp), "ACK", seq, token, "{\"status\":\"ack\"}");
            sendto(udp_sock_global, resp, strlen(resp), 0,
                   (struct sockaddr *) &origen_addr, addr_len);

            log_evento("UDP", ip, puerto, "EVENT", seq, origen_id,
                       ya_procesado ? "ACK_DUPLICADO" : "ACK");
        } else {
            log_evento("UDP", ip, puerto, tipo, seq, origen_id, "UNKNOWN_TYPE");
        }
    }
    return NULL;
}

/* ---------------------------------------------------------------------
 * main
 * --------------------------------------------------------------------- */

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s <puerto> <archivo_de_logs>\n", argv[0]);
        return 1;
    }
    int puerto = atoi(argv[1]);
    archivo_log = fopen(argv[2], "a");
    if (!archivo_log) {
        fprintf(stderr, "Aviso: no se pudo abrir el archivo de logs (%s), se continua solo con consola.\n",
                argv[2]);
    }

    srand((unsigned int) time(NULL));

    /* --- Socket TCP --- */
    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) { perror("socket TCP"); return 1; }

    int opt = 1;
    setsockopt(tcp_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in dir_tcp;
    memset(&dir_tcp, 0, sizeof(dir_tcp));
    dir_tcp.sin_family = AF_INET;
    dir_tcp.sin_addr.s_addr = INADDR_ANY;
    dir_tcp.sin_port = htons(puerto);

    if (bind(tcp_sock, (struct sockaddr *) &dir_tcp, sizeof(dir_tcp)) < 0) {
        perror("bind TCP"); return 1;
    }
    if (listen(tcp_sock, 16) < 0) {
        perror("listen"); return 1;
    }

    /* --- Socket UDP (mismo numero de puerto, transporte distinto) --- */
    udp_sock_global = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock_global < 0) { perror("socket UDP"); return 1; }

    struct sockaddr_in dir_udp;
    memset(&dir_udp, 0, sizeof(dir_udp));
    dir_udp.sin_family = AF_INET;
    dir_udp.sin_addr.s_addr = INADDR_ANY;
    dir_udp.sin_port = htons(puerto);

    if (bind(udp_sock_global, (struct sockaddr *) &dir_udp, sizeof(dir_udp)) < 0) {
        perror("bind UDP"); return 1;
    }

    pthread_t hilo_udp;
    pthread_create(&hilo_udp, NULL, escuchar_udp, NULL);
    pthread_detach(hilo_udp);

    printf("Servidor SMDP/1.0 escuchando en el puerto %d (TCP y UDP)\n", puerto);

    for (;;) {
        struct sockaddr_in dir_cliente;
        socklen_t len_cliente = sizeof(dir_cliente);
        int fd_cliente = accept(tcp_sock, (struct sockaddr *) &dir_cliente, &len_cliente);
        if (fd_cliente < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        ClienteTCP *cli = malloc(sizeof(ClienteTCP));
        cli->fd = fd_cliente;
        cli->addr = dir_cliente;

        pthread_t hilo;
        if (pthread_create(&hilo, NULL, manejar_cliente_tcp, cli) != 0) {
            perror("pthread_create");
            close(fd_cliente);
            free(cli);
            continue;
        }
        pthread_detach(hilo);
    }

    return 0;
}
