/* agent_400.c - RemoteOps Agent (server)  */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/*  Personalised values (IT24103400) */
#define REG_NO        "IT24103400"
#define AGENT_PORT    9410                /* 7000 + 2410 */
#define SID           "0043"              
#define AUTH_TOKEN    "OPS-3400"
#define LOG_FILE      "remoteops_IT24103400.log"
#define STORAGE_ROOT  "./agentfiles"
#define STORAGE_DIR   "./agentfiles/IT24103400"

#define MAX_LINE 1024
#define IN_BUF   4096

/* One of these per connected Controller; owned by that client's thread */
typedef struct {
    int    fd;
    char   ip[INET_ADDRSTRLEN];
    int    port;
    int    authed;
    char   inbuf[IN_BUF];   /* leftover bytes between recv() calls */
    size_t inlen;
} session_t;

/* Logging (thread-safe)  */
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_event(session_t *s, const char *fmt, ...)
{
    char msg[512], ts[32], who[64];
    time_t now = time(NULL);
    struct tm tmv;
    va_list ap;

    localtime_r(&now, &tmv);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tmv);
    if (s) snprintf(who, sizeof who, "%s:%d", s->ip, s->port);
    else   snprintf(who, sizeof who, "SERVER");

    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&log_lock);          /* one thread writes at a time */
    FILE *f = fopen(LOG_FILE, "a");
    if (f) { fprintf(f, "[%s] %s %s\n", ts, who, msg); fclose(f); }
    printf("[%s] %s %s\n", ts, who, msg);
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}

/* Sending helpers  */
/* send() may write fewer bytes than asked, so loop until everything is sent */
static int send_all(int fd, const char *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

/* Every response goes through here, so the " SID:0043\n" tag is never forgotten */
static int send_resp(session_t *s, const char *fmt, ...)
{
    char body[4096], out[4096 + 16];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);

    int n = snprintf(out, sizeof out, "%s SID:%s\n", body, SID);
    return send_all(s->fd, out, (size_t)n);
}

/*  Per-client thread (temporary stub, replaced in Segment 3) ---------- */
static void *client_thread(void *arg)
{
    session_t *s = arg;
    char buf[256];

    log_event(s, "CONNECT");
    while (recv(s->fd, buf, sizeof buf, 0) > 0) { /* discard for now */ }
    log_event(s, "DISCONNECT");

    close(s->fd);
    free(s);
    return NULL;
}

/* ---------- main: create listening socket, accept loop ---------- */
int main(void)
{
    signal(SIGPIPE, SIG_IGN);               /* a dead client must not kill the Agent */

    mkdir(STORAGE_ROOT, 0755);
    mkdir(STORAGE_DIR, 0755);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(AGENT_PORT);

    if (bind(srv, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(srv, 16) < 0) { perror("listen"); return 1; }

    log_event(NULL, "Agent started for %s, listening on port %d", REG_NO, AGENT_PORT);

    for (;;) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int cfd = accept(srv, (struct sockaddr *)&ca, &cl);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        session_t *s = calloc(1, sizeof *s);
        if (!s) { close(cfd); continue; }
        s->fd   = cfd;
        s->port = ntohs(ca.sin_port);
        inet_ntop(AF_INET, &ca.sin_addr, s->ip, sizeof s->ip);

        pthread_t t;
        if (pthread_create(&t, NULL, client_thread, s) != 0) {
            close(cfd);
            free(s);
            continue;
        }
        pthread_detach(t);                  /* no join needed; thread cleans itself up */
    }
}
