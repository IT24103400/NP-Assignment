
/* controller_400.c - RemoteOps Controller (client) | Reg No: IT24103400 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <libgen.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
 
#define DEFAULT_PORT 9410
#define MAX_LINE     1024
#define IN_BUF       65536      /* large: LISTPROC replies can be long */
#define XFER_BUF     8192
#define DOWNLOAD_DIR "downloads"
 
typedef struct {
    int    fd;
    char   buf[IN_BUF];
    size_t len;
} conn_t;
 
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
 
/* Same framing idea as the Agent: returns 1 = line, 0 = closed, -1 = error.
 * Bytes after the newline stay in c->buf (needed for GET: file bytes follow the reply line). */
static int read_line(conn_t *c, char *line, size_t max)
{
    for (;;) {
        char *nl = memchr(c->buf, '\n', c->len);
        if (nl) {
            size_t len      = (size_t)(nl - c->buf);
            size_t copy     = len < max - 1 ? len : max - 1;
            size_t consumed = len + 1;
 
            memcpy(line, c->buf, copy);
            line[copy] = '\0';
            memmove(c->buf, c->buf + consumed, c->len - consumed);
            c->len -= consumed;
            return 1;
        }
        if (c->len == sizeof c->buf) return -1;
 
        ssize_t n = recv(c->fd, c->buf + c->len, sizeof c->buf - c->len, 0);
        if (n == 0) return 0;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        c->len += (size_t)n;
    }
}
 
/* Remove trailing spaces in place */
static void rtrim(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && s[n - 1] == ' ') s[--n] = '\0';
}
 
/*  Plain one-line command: send, read one reply line 
 * Returns 0 = continue, 1 = Agent said BYE, -1 = connection lost */
static int do_simple(conn_t *c, const char *input, char *first_out)
{
    char out[MAX_LINE + 2], resp[IN_BUF];
    int n = snprintf(out, sizeof out, "%s\n", input);
    if (send_all(c->fd, out, (size_t)n) < 0) {
        printf("Send failed: connection lost\n");
        return -1;
    }
 
    int r = read_line(c, resp, sizeof resp);
    if (r <= 0) { printf("Connection closed by Agent\n"); return -1; }
    if (first_out) snprintf(first_out, 64, "%.63s", resp);   /* caller may inspect the reply */
    printf("%s\n", resp);
 
    return strncmp(resp, "OK BYE", 6) == 0 ? 1 : 0;
}
 
/* PUT <localfile> 
 * Sends "PUT <name> <size>\n" followed immediately by exactly <size> raw bytes. */
static int do_put(conn_t *c, char *path)
{
    rtrim(path);
    struct stat st;
    if (path[0] == '\0') { printf("Usage: PUT <localfile>\n"); return 0; }
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        printf("Local file not found: %s\n", path);
        return 0;
    }
    FILE *f = fopen(path, "rb");
    if (!f) { printf("Cannot open %s: %s\n", path, strerror(errno)); return 0; }
 
    char copy[512];
    snprintf(copy, sizeof copy, "%s", path);
    const char *name = basename(copy);      /* send only the file name, not the directory */
    unsigned long long size = (unsigned long long)st.st_size;
 
    char header[MAX_LINE];
    int n = snprintf(header, sizeof header, "PUT %s %llu\n", name, size);
    if (send_all(c->fd, header, (size_t)n) < 0) { fclose(f); printf("Send failed\n"); return -1; }
 
    char buf[XFER_BUF];
    unsigned long long sent = 0;
    while (sent < size) {
        size_t want = size - sent < sizeof buf ? (size_t)(size - sent) : sizeof buf;
        size_t got = fread(buf, 1, want, f);
        if (got == 0 || send_all(c->fd, buf, got) < 0) {
            fclose(f);
            printf("Upload failed after %llu of %llu bytes\n", sent, size);
            return -1;
        }
        sent += got;
    }
    fclose(f);
 
    char resp[IN_BUF];
    int r = read_line(c, resp, sizeof resp);
    if (r <= 0) { printf("Connection closed by Agent\n"); return -1; }
    printf("%s\n", resp);
    return 0;
}
 
/* Read exactly <size> bytes into f: first what is already buffered, then from the socket */
static int recv_file(conn_t *c, FILE *f, unsigned long long size)
{
    unsigned long long remaining = size;
 
    size_t take = c->len < remaining ? c->len : (size_t)remaining;
    if (take > 0) {
        if (fwrite(c->buf, 1, take, f) != take) return -1;
        memmove(c->buf, c->buf + take, c->len - take);
        c->len    -= take;
        remaining -= take;
    }
 
    char buf[XFER_BUF];
    while (remaining > 0) {
        size_t want = remaining < sizeof buf ? (size_t)remaining : sizeof buf;
        ssize_t n = recv(c->fd, buf, want, 0);
        if (n == 0) return -1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (fwrite(buf, 1, (size_t)n, f) != (size_t)n) return -1;
        remaining -= (unsigned long long)n;
    }
    return 0;
}
 
/* GET <filename> 
 * Reply line "OK FILE_SEND <name> <size>" is followed by exactly <size> raw bytes.
 * Saved to ./downloads/<name> so the original file is never overwritten. */
static int do_get(conn_t *c, char *name)
{
    rtrim(name);
    if (name[0] == '\0') { printf("Usage: GET <filename>\n"); return 0; }
    if (strchr(name, '/')) { printf("Give a plain file name, not a path\n"); return 0; }
 
    char out[MAX_LINE + 2], resp[IN_BUF];
    int n = snprintf(out, sizeof out, "GET %s\n", name);
    if (send_all(c->fd, out, (size_t)n) < 0) { printf("Send failed\n"); return -1; }
 
    int r = read_line(c, resp, sizeof resp);
    if (r <= 0) { printf("Connection closed by Agent\n"); return -1; }
    printf("%s\n", resp);
 
    if (strncmp(resp, "OK FILE_SEND ", 13) != 0) return 0;     /* an ERR line: nothing follows */
 
    char fname[256];
    unsigned long long size;
    if (sscanf(resp, "OK FILE_SEND %255s %llu", fname, &size) != 2) {
        printf("Malformed reply from Agent\n");
        return -1;
    }
 
    mkdir(DOWNLOAD_DIR, 0755);
    char path[512];
    snprintf(path, sizeof path, "%s/%s", DOWNLOAD_DIR, name);
    FILE *f = fopen(path, "wb");
    if (!f) { printf("Cannot create %s: %s\n", path, strerror(errno)); return -1; }
 
    if (recv_file(c, f, size) < 0) {
        fclose(f);
        printf("Download failed (connection lost or disk error)\n");
        return -1;
    }
    fclose(f);
    printf("Saved %s (%llu bytes)\n", path, size);
    return 0;
}
 
/* UDP monitoring receiver 
 * MONITOR START <udp_port>: bind a UDP socket on that port, then ask the Agent to
 * send datagrams to it. A background thread prints each datagram as it arrives
 * while the prompt stays usable. MONITOR STOP / QUIT / exit stops the thread. */
static int           udp_fd = -1;
static pthread_t     udp_thread;
static volatile int  udp_running = 0;
 
static void *udp_receiver(void *arg)
{
    (void)arg;
    char buf[512];
    while (udp_running) {
        ssize_t n = recvfrom(udp_fd, buf, sizeof buf - 1, 0, NULL, NULL);
        if (n < 0) continue;            /* 500 ms timeout or EINTR: re-check udp_running */
        buf[n] = '\0';
        printf("\r\033[K[UDP] %s\nremoteops> ", buf);
        fflush(stdout);
    }
    return NULL;
}
 
/* Returns 0 on success, -1 on failure, -2 if already receiving */
static int start_udp(int port)
{
    if (udp_running) return -2;
 
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("udp socket"); return -1; }
 
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port        = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        printf("Cannot listen on UDP port %d: %s\n", port, strerror(errno));
        close(fd);
        return -1;
    }
 
    struct timeval tv = { 0, 500000 };  /* recvfrom wakes every 0.5 s so we can stop cleanly */
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
 
    udp_fd = fd;
    udp_running = 1;
    if (pthread_create(&udp_thread, NULL, udp_receiver, NULL) != 0) {
        udp_running = 0;
        close(fd);
        udp_fd = -1;
        return -1;
    }
    return 0;
}
 
static void stop_udp(void)
{
    if (!udp_running) return;
    udp_running = 0;
    pthread_join(udp_thread, NULL);
    close(udp_fd);
    udp_fd = -1;
}
 
/* MONITOR START <port> | MONITOR STOP: manage the local UDP receiver around the TCP command */
static int do_monitor(conn_t *c, const char *input)
{
    char sub[16] = "", portstr[16] = "";
    int n = sscanf(input, "MONITOR %15s %15s", sub, portstr);
    int started_here = 0;
 
    if (n >= 2 && strcmp(sub, "START") == 0) {
        int port = atoi(portstr);
        if (port >= 1 && port <= 65535) {       /* bad ports are left for the Agent to reject */
            int r = start_udp(port);
            if (r == -2) { printf("Already receiving monitoring data; send MONITOR STOP first\n"); return 0; }
            if (r < 0) return 0;
            started_here = 1;
        }
    }
 
    char first[64] = "";
    int rc = do_simple(c, input, first);
 
    if (started_here && strncmp(first, "OK MONITOR_STARTED", 18) != 0)
        stop_udp();                             /* the Agent refused: no stream is coming */
    if (n >= 1 && strcmp(sub, "STOP") == 0 && strncmp(first, "OK MONITOR_STOPPED", 18) == 0)
        stop_udp();
    return rc;
}
 
int main(int argc, char *argv[])
{
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port         = (argc > 2) ? atoi(argv[2]) : DEFAULT_PORT;
 
    signal(SIGPIPE, SIG_IGN);
 
    static conn_t c;                        /* static: keeps the 64 KB buffer off the stack */
    c.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (c.fd < 0) { perror("socket"); return 1; }
 
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "Invalid IPv4 address: %s\n", host);
        return 1;
    }
    if (connect(c.fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
        perror("connect");
        return 1;
    }
    printf("Connected to %s:%d\n", host, port);
    printf("Commands: AUTH <token>, SYSINFO, LISTPROC, EXEC <name>, PUT <file>, GET <file>,\n          MONITOR START <udp_port>, MONITOR STOP, QUIT\n");
 
    char input[MAX_LINE];
 
    for (;;) {
        printf("remoteops> ");
        fflush(stdout);
        if (!fgets(input, sizeof input, stdin)) break;      
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0] == '\0') continue;
 
        int rc;
        if      (strncmp(input, "PUT ", 4) == 0) rc = do_put(&c, input + 4);
        else if (strncmp(input, "GET ", 4) == 0) rc = do_get(&c, input + 4);
        else if (strncmp(input, "MONITOR ", 8) == 0) rc = do_monitor(&c, input);
        else                                     rc = do_simple(&c, input, NULL);
 
        if (rc != 0) break;                     /* 1 = BYE, -1 = connection lost */
    }
 
    stop_udp();
    close(c.fd);
    return 0;
}
 
