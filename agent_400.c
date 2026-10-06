

/* agent_400.c - RemoteOps Agent (server) | Reg No: IT24103400 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <signal.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
 

#define REG_NO        "IT24103400"
#define AGENT_PORT    9410                
#define SID           "0043"              
#define AUTH_TOKEN    "OPS-3400"
#define LOG_FILE      "remoteops_IT24103400.log"
#define STORAGE_ROOT  "./agentfiles"
#define STORAGE_DIR   "./agentfiles/IT24103400"
 
#define MAX_LINE      1024
#define IN_BUF        4096
#define XFER_BUF      8192
#define MAX_FILE_SIZE (50ULL * 1024 * 1024)   
#define MAX_DRAIN     (1ULL << 30)            
#define MONITOR_INTERVAL_SEC 2                
 
/* One of these per connected Controller; owned by that client's thread */
typedef struct {
    int    fd;
    char   ip[INET_ADDRSTRLEN];
    int    port;
    int    authed;
    char   inbuf[IN_BUF];   /* leftover bytes between recv() calls */
    size_t inlen;
 
    /* UDP monitoring: at most one stream per session, sent by its own thread */
    int             mon_active;     /* 1 while a monitor thread is running */
    int             mon_stop;       /* set to 1 to ask the monitor thread to finish */
    int             mon_sock;       /* UDP socket used by the monitor thread */
    int             mon_port;       /* Controller's UDP port */
    pthread_t       mon_thread;
    pthread_mutex_t mon_lock;       /* protects mon_stop, used with mon_cond */
    pthread_cond_t  mon_cond;       /* lets MONITOR STOP wake the thread at once */
} session_t;
 
/* Logging (thread-safe)  */
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
 
/* Writes a timestamped line to the log file and to the console */
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
 
    pthread_mutex_lock(&log_lock);          /* only one thread writes at a time */
    FILE *f = fopen(LOG_FILE, "a");
    if (f) { fprintf(f, "[%s] %s %s\n", ts, who, msg); fclose(f); }
    printf("[%s] %s %s\n", ts, who, msg);
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}
 
/* Sending helpers */
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
 
/* Line reader (framing)
 * Returns: 1 = got a line, 0 = client closed, -1 = recv error, -2 = line too long.
 * Handles: partial lines across recv() calls, and several lines in one recv().
 * Bytes after the newline stay in s->inbuf (essential for PUT, where
 * raw file bytes follow the command line immediately). */
static int read_line(session_t *s, char *line, size_t max)
{
    for (;;) {
        char *nl = memchr(s->inbuf, '\n', s->inlen);
        if (nl) {
            size_t len      = (size_t)(nl - s->inbuf);
            size_t copy     = len < max - 1 ? len : max - 1;
            size_t consumed = len + 1;
 
            memcpy(line, s->inbuf, copy);
            line[copy] = '\0';
            if (copy > 0 && line[copy - 1] == '\r') line[copy - 1] = '\0';
 
            memmove(s->inbuf, s->inbuf + consumed, s->inlen - consumed);
            s->inlen -= consumed;
            return 1;
        }
 
        if (s->inlen == sizeof s->inbuf) return -2;     /* buffer full, no newline */
 
        ssize_t n = recv(s->fd, s->inbuf + s->inlen, sizeof s->inbuf - s->inlen, 0);
        if (n == 0) return 0;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        s->inlen += (size_t)n;
    }
}
 
/* DAY 2: COMMAND HANDLERS */
/* Every handler returns 0 normally, or -1 if the connection is lost
 * (the client thread then ends the session). */
 
/* SYSINFO*/
/* Fills out with "<cpu_load> <mem_used_mb> <uptime_sec>" read from /proc.
 * Kept separate so the UDP monitor (Day 3) can reuse it. */
static void get_sysinfo(char *out, size_t n)
{
    double load = 0.0, up = 0.0;
    long mem_total_kb = 0, mem_avail_kb = 0;
    FILE *f;
 
    f = fopen("/proc/loadavg", "r");
    if (f) { if (fscanf(f, "%lf", &load) != 1) load = 0.0; fclose(f); }
 
    f = fopen("/proc/uptime", "r");
    if (f) { if (fscanf(f, "%lf", &up) != 1) up = 0.0; fclose(f); }
 
    f = fopen("/proc/meminfo", "r");
    if (f) {
        char line[128], key[64];
        long val;
        while (fgets(line, sizeof line, f)) {
            if (sscanf(line, "%63[^:]: %ld", key, &val) == 2) {
                if (strcmp(key, "MemTotal") == 0)          mem_total_kb = val;
                else if (strcmp(key, "MemAvailable") == 0) mem_avail_kb = val;
            }
        }
        fclose(f);
    }
 
    snprintf(out, n, "%.2f %ld %ld", load, (mem_total_kb - mem_avail_kb) / 1024, (long)up);
}
 
static int handle_sysinfo(session_t *s)
{
    char info[128];
    get_sysinfo(info, sizeof info);
    return send_resp(s, "OK SYSINFO %s", info);
}
 
/* LISTPROC */
/* Snapshot from "ps", returned as one line: pid:name,pid:name,... */
static int handle_listproc(session_t *s)
{
    FILE *p = popen("ps -eo pid=,comm=", "r");
    if (!p) return send_resp(s, "ERR 008 COMMAND_FAILED");
 
    char list[3600], line[256];
    size_t used = 0;
    list[0] = '\0';
 
    while (fgets(line, sizeof line, p)) {
        int pid;
        char name[128];
        if (sscanf(line, "%d %127s", &pid, name) != 2) continue;
 
        int w = snprintf(list + used, sizeof list - used, "%s%d:%s",
                         used ? "," : "", pid, name);
        if (w < 0 || (size_t)w >= sizeof list - used) break;   /* list full: stop here */
        used += (size_t)w;
    }
    list[used] = '\0';          /* drop any half-written entry */
    pclose(p);
 
    return send_resp(s, "OK PROCS %s", list);
}
 
/* EXEC (fixed whitelist) */
/* The client's text is only ever compared against this table. It is never
 * passed to the shell, so nothing outside these five commands can run. */
static const struct { const char *name; const char *cmd; } exec_table[] = {
    { "DATE",     "date"      },
    { "UPTIME",   "uptime"    },
    { "DISKFREE", "df -h /"   },
    { "HOSTNAME", "hostname"  },
    { "WHOAMI",   "whoami"    },
};
 
static int handle_exec(session_t *s, const char *name)
{
    const char *cmd = NULL;
    for (size_t i = 0; i < sizeof exec_table / sizeof exec_table[0]; i++) {
        if (strcmp(name, exec_table[i].name) == 0) { cmd = exec_table[i].cmd; break; }
    }
    if (!cmd) {
        log_event(s, "EXEC rejected: '%s' is not whitelisted", name);
        return send_resp(s, "ERR 002 COMMAND_NOT_ALLOWED");
    }
 
    FILE *p = popen(cmd, "r");
    if (!p) return send_resp(s, "ERR 008 COMMAND_FAILED");
 
    char out[1024];
    size_t n = fread(out, 1, sizeof out - 1, p);
    out[n] = '\0';
    pclose(p);
 
    /* The reply must be ONE line, so turn newlines into spaces and trim the end */
    for (size_t i = 0; i < n; i++)
        if (out[i] == '\n' || out[i] == '\r') out[i] = ' ';
    while (n > 0 && out[n - 1] == ' ') out[--n] = '\0';
 
    return send_resp(s, "OK EXEC_RESULT %s", out);
}
 
/* File transfer helpers */
/* Allow only [A-Za-z0-9._-], max 100 chars, not starting with '.'.
 * This blocks path traversal such as "../x" or "/etc/passwd". */
static int valid_filename(const char *name)
{
    size_t len = strlen(name);
    if (len == 0 || len > 100 || name[0] == '.') return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!isalnum(c) && c != '.' && c != '_' && c != '-') return 0;
    }
    return 1;
}
 
/* Read and throw away 'count' bytes, so the byte stream stays in sync
 * after we reject a PUT whose data is already on its way. */
static int discard_bytes(session_t *s, unsigned long long count)
{
    char buf[XFER_BUF];
 
    size_t take = s->inlen < count ? s->inlen : (size_t)count;   /* leftover first */
    memmove(s->inbuf, s->inbuf + take, s->inlen - take);
    s->inlen -= take;
    count    -= take;
 
    while (count > 0) {
        size_t want = count < sizeof buf ? (size_t)count : sizeof buf;
        ssize_t n = recv(s->fd, buf, want, 0);
        if (n == 0) return -1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        count -= (unsigned long long)n;
    }
    return 0;
}
 
/* Reject a PUT: swallow its data (if reasonable), then send the error */
static int reject_put(session_t *s, unsigned long long size, const char *err)
{
    if (size > MAX_DRAIN) {                 /* too much to swallow: reply and close */
        send_resp(s, "%s", err);
        return -1;
    }
    if (discard_bytes(s, size) < 0) return -1;
    return send_resp(s, "%s", err);
}
 
/* PUT <filename> <filesize> + raw bytes */
static int handle_put(session_t *s, char *args)
{
    char *sp = strrchr(args, ' ');
    if (!sp) return send_resp(s, "ERR 009 BAD_ARGUMENTS");
    *sp = '\0';
    char *name = args;
    char *sz   = sp + 1;
 
    size_t nl = strlen(name);
    while (nl > 0 && name[nl - 1] == ' ') name[--nl] = '\0';
 
    if (*sz < '0' || *sz > '9') return send_resp(s, "ERR 009 BAD_ARGUMENTS");
    char *end;
    errno = 0;
    unsigned long long size = strtoull(sz, &end, 10);
    if (*end != '\0' || errno == ERANGE) return send_resp(s, "ERR 009 BAD_ARGUMENTS");
 
    if (size > MAX_FILE_SIZE) {
        log_event(s, "PUT rejected: %s is %llu bytes (limit %llu)", name, size, MAX_FILE_SIZE);
        return reject_put(s, size, "ERR 004 FILE_TOO_LARGE");
    }
    if (!valid_filename(name)) {
        log_event(s, "PUT rejected: bad filename '%s'", name);
        return reject_put(s, size, "ERR 010 BAD_FILENAME");
    }
 
    /* Write into a temporary file, then rename() it into place when complete.
     * Other clients never see a half-written file, and two clients uploading
     * the same name at once cannot corrupt each other (last rename wins). */
    char path[256], tmp[256];
    snprintf(path, sizeof path, "%s/%s", STORAGE_DIR, name);
    snprintf(tmp,  sizeof tmp,  "%s/.part.%d", STORAGE_DIR, s->fd);
 
    FILE *f = fopen(tmp, "wb");
    if (!f) {
        log_event(s, "PUT %s failed: cannot create file (%s)", name, strerror(errno));
        return reject_put(s, size, "ERR 011 STORAGE_ERROR");
    }
 
    log_event(s, "PUT %s start (%llu bytes)", name, size);
 
    unsigned long long remaining = size;
    int write_err = 0;
    char buf[XFER_BUF];
 
    /* Step 1: bytes that already arrived together with the command line */
    size_t take = s->inlen < remaining ? s->inlen : (size_t)remaining;
    if (take > 0) {
        if (fwrite(s->inbuf, 1, take, f) != take) write_err = 1;
        memmove(s->inbuf, s->inbuf + take, s->inlen - take);
        s->inlen  -= take;
        remaining -= take;
    }
 
    /* Step 2: the rest comes from the socket, in however many recv() calls it takes */
    while (remaining > 0) {
        size_t want = remaining < sizeof buf ? (size_t)remaining : sizeof buf;
        ssize_t n = recv(s->fd, buf, want, 0);
        if (n == 0 || (n < 0 && errno != EINTR)) {
            fclose(f);
            remove(tmp);                    /* never keep a partial upload */
            log_event(s, "PUT %s aborted: connection lost with %llu bytes missing",
                      name, remaining);
            return -1;
        }
        if (n < 0) continue;                /* EINTR */
        if (!write_err && fwrite(buf, 1, (size_t)n, f) != (size_t)n) write_err = 1;
        remaining -= (unsigned long long)n;
    }
 
    if (fclose(f) != 0) write_err = 1;
    if (write_err || rename(tmp, path) != 0) {
        remove(tmp);
        log_event(s, "PUT %s failed: could not store file", name);
        return send_resp(s, "ERR 011 STORAGE_ERROR");
    }
 
    log_event(s, "PUT %s complete (%llu bytes stored)", name, size);
    return send_resp(s, "OK FILE_RECEIVED %s", name);
}
 
/* GET <filename> */
static int handle_get(session_t *s, char *args)
{
    char *name = args;
    size_t nl = strlen(name);
    while (nl > 0 && name[nl - 1] == ' ') name[--nl] = '\0';
 
    if (!valid_filename(name)) {
        log_event(s, "GET rejected: bad filename '%s'", name);
        return send_resp(s, "ERR 010 BAD_FILENAME");
    }
 
    char path[256];
    snprintf(path, sizeof path, "%s/%s", STORAGE_DIR, name);
 
    struct stat st;
    FILE *f = fopen(path, "rb");
    if (!f || fstat(fileno(f), &st) != 0 || !S_ISREG(st.st_mode)) {
        if (f) fclose(f);
        log_event(s, "GET %s failed: file not found", name);
        return send_resp(s, "ERR 005 FILE_NOT_FOUND");
    }
 
    unsigned long long size = (unsigned long long)st.st_size;
    if (send_resp(s, "OK FILE_SEND %s %llu", name, size) < 0) { fclose(f); return -1; }
 
    log_event(s, "GET %s start (%llu bytes)", name, size);
 
    char buf[XFER_BUF];
    unsigned long long sent = 0;
    while (sent < size) {
        size_t want = size - sent < sizeof buf ? (size_t)(size - sent) : sizeof buf;
        size_t n = fread(buf, 1, want, f);
        if (n == 0 || send_all(s->fd, buf, n) < 0) {
            fclose(f);
            log_event(s, "GET %s aborted after %llu of %llu bytes", name, sent, size);
            return -1;                      /* we promised <size> bytes; close the session */
        }
        sent += n;
    }
    fclose(f);
 
    log_event(s, "GET %s complete (%llu bytes sent)", name, size);
    return 0;
}
 
/* UDP monitoring: MONITOR START <udp_port> / MONITOR STOP */
/* Runs in its own thread: send one SYSINFO datagram, sleep for the interval, repeat.
 * The sleep is a timed wait on a condition variable, so a stop request wakes the
 * thread immediately instead of waiting out the interval. */
static void *monitor_thread(void *arg)
{
    session_t *s = arg;
    struct sockaddr_in dst;
    memset(&dst, 0, sizeof dst);
    dst.sin_family = AF_INET;
    dst.sin_port   = htons((unsigned short)s->mon_port);
    inet_pton(AF_INET, s->ip, &dst.sin_addr);      /* the Controller's own IP */
 
    pthread_mutex_lock(&s->mon_lock);
    while (!s->mon_stop) {
        char info[128], msg[192];
        get_sysinfo(info, sizeof info);
        int n = snprintf(msg, sizeof msg, "SYSINFO %s SID:%s", info, SID);
        sendto(s->mon_sock, msg, (size_t)n, 0, (struct sockaddr *)&dst, sizeof dst);
 
        struct timespec deadline;
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec += MONITOR_INTERVAL_SEC;
        pthread_cond_timedwait(&s->mon_cond, &s->mon_lock, &deadline);
    }
    pthread_mutex_unlock(&s->mon_lock);
    return NULL;
}
 
/* Safe to call at any time (does nothing if no stream is running).
 * Called by MONITOR STOP, QUIT, and when the session ends for any reason. */
static void stop_monitor(session_t *s)
{
    if (!s->mon_active) return;
 
    pthread_mutex_lock(&s->mon_lock);
    s->mon_stop = 1;
    pthread_cond_signal(&s->mon_cond);
    pthread_mutex_unlock(&s->mon_lock);
 
    pthread_join(s->mon_thread, NULL);
    close(s->mon_sock);
    s->mon_active = 0;
    log_event(s, "MONITOR stopped (UDP port %d)", s->mon_port);
}
 
static int handle_monitor(session_t *s, char *args)
{
    char sub[16] = "", portstr[16] = "", extra[16] = "";
    int n = sscanf(args, "%15s %15s %15s", sub, portstr, extra);
 
    if (n == 1 && strcmp(sub, "STOP") == 0) {
        if (!s->mon_active) return send_resp(s, "ERR 013 MONITOR_NOT_RUNNING");
        stop_monitor(s);
        return send_resp(s, "OK MONITOR_STOPPED");
    }
 
    if (n == 2 && strcmp(sub, "START") == 0) {
        char *end;
        long port = strtol(portstr, &end, 10);
        if (*end != '\0' || port < 1 || port > 65535)
            return send_resp(s, "ERR 009 BAD_ARGUMENTS");
        if (s->mon_active)
            return send_resp(s, "ERR 012 MONITOR_ALREADY_RUNNING");
 
        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0) return send_resp(s, "ERR 014 MONITOR_FAILED");
 
        s->mon_sock = sock;
        s->mon_port = (int)port;
        s->mon_stop = 0;
        if (pthread_create(&s->mon_thread, NULL, monitor_thread, s) != 0) {
            close(sock);
            return send_resp(s, "ERR 014 MONITOR_FAILED");
        }
        s->mon_active = 1;
        log_event(s, "MONITOR started: UDP to %s:%d every %d s",
                  s->ip, s->mon_port, MONITOR_INTERVAL_SEC);
        return send_resp(s, "OK MONITOR_STARTED");
    }
 
    return send_resp(s, "ERR 009 BAD_ARGUMENTS");
}
 
/* Per-client thread */
static void *client_thread(void *arg)
{
    session_t *s = arg;
    char line[MAX_LINE];
 
    log_event(s, "CONNECT");
 
    /* Per-session lock and condition variable for the UDP monitor thread.
     * The condition variable uses the monotonic clock so a system clock change
     * cannot stretch or shrink the monitoring interval. */
    pthread_condattr_t cattr;
    pthread_condattr_init(&cattr);
    pthread_condattr_setclock(&cattr, CLOCK_MONOTONIC);
    pthread_cond_init(&s->mon_cond, &cattr);
    pthread_condattr_destroy(&cattr);
    pthread_mutex_init(&s->mon_lock, NULL);
 
    for (;;) {
        int r = read_line(s, line, sizeof line);
        if (r == 0)  { log_event(s, "DISCONNECT (client closed connection)"); break; }
        if (r == -1) { log_event(s, "DISCONNECT (recv error: %s)", strerror(errno)); break; }
        if (r == -2) {
            send_resp(s, "ERR 006 LINE_TOO_LONG");
            log_event(s, "DISCONNECT (line too long)");
            break;
        }
        if (line[0] == '\0') continue;
 
        /* Split "COMMAND args" in place */
        char *cmd = line, *args = line + strlen(line);
        char *sp = strchr(line, ' ');
        if (sp) {
            *sp = '\0';
            args = sp + 1;
            while (*args == ' ') args++;
        }
 
        /* Never write the token to the log */
        if (strcmp(cmd, "AUTH") == 0) log_event(s, "CMD AUTH ****");
        else                          log_event(s, "CMD %s %s", cmd, args);
 
        if (strcmp(cmd, "AUTH") == 0) {
            if (strcmp(args, AUTH_TOKEN) == 0) {
                s->authed = 1;
                send_resp(s, "OK AUTHENTICATED");
                log_event(s, "AUTH success");
            } else {
                send_resp(s, "ERR 001 AUTH_FAILED");
                log_event(s, "AUTH failed");
            }
            continue;
        }
 
        /* Everything else needs a successful AUTH first */
        if (!s->authed) {
            send_resp(s, "ERR 003 NOT_AUTHENTICATED");
            continue;
        }
 
        if (strcmp(cmd, "QUIT") == 0) {
            stop_monitor(s);                /* QUIT also ends the UDP stream */
            send_resp(s, "OK BYE");
            log_event(s, "QUIT");
            break;
        }
 
        /* Command dispatch */
        int rc;
        if      (strcmp(cmd, "SYSINFO")  == 0) rc = handle_sysinfo(s);
        else if (strcmp(cmd, "LISTPROC") == 0) rc = handle_listproc(s);
        else if (strcmp(cmd, "EXEC")     == 0) rc = handle_exec(s, args);
        else if (strcmp(cmd, "PUT")      == 0) rc = handle_put(s, args);
        else if (strcmp(cmd, "GET")      == 0) rc = handle_get(s, args);
        else if (strcmp(cmd, "MONITOR")  == 0) rc = handle_monitor(s, args);
        else                                   rc = send_resp(s, "ERR 007 UNKNOWN_COMMAND");
 
        if (rc < 0) { log_event(s, "DISCONNECT (connection lost)"); break; }
    }
 
    stop_monitor(s);                        /* safety net: any exit path ends the UDP stream */
    pthread_cond_destroy(&s->mon_cond);
    pthread_mutex_destroy(&s->mon_lock);
    close(s->fd);
    free(s);
    return NULL;
}
 
/* main: create listening socket, accept loop */
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
 
