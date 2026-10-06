

/* controller_400.c - RemoteOps Controller (client) | Reg No: IT24103400 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <libgen.h>
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
 
/* Plain one-line command: send, read one reply line 
 * Returns 0 = continue, 1 = Agent said BYE, -1 = connection lost */
static int do_simple(conn_t *c, const char *input)
{
    char out[MAX_LINE + 2], resp[IN_BUF];
    int n = snprintf(out, sizeof out, "%s\n", input);
    if (send_all(c->fd, out, (size_t)n) < 0) {
        printf("Send failed: connection lost\n");
        return -1;
    }
 
    int r = read_line(c, resp, sizeof resp);
    if (r <= 0) { printf("Connection closed by Agent\n"); return -1; }
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
    printf("Commands: AUTH <token>, SYSINFO, LISTPROC, EXEC <name>, PUT <file>, GET <file>, QUIT\n");
 
    char input[MAX_LINE];
 
    for (;;) {
        printf("remoteops> ");
        fflush(stdout);
        if (!fgets(input, sizeof input, stdin)) break;      /* Ctrl+D */
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0] == '\0') continue;
 
        int rc;
        if      (strncmp(input, "PUT ", 4) == 0) rc = do_put(&c, input + 4);
        else if (strncmp(input, "GET ", 4) == 0) rc = do_get(&c, input + 4);
        else                                     rc = do_simple(&c, input);
 
        if (rc != 0) break;                     /* 1 = BYE, -1 = connection lost */
    }
 
    close(c.fd);
    return 0;
}
 
