/* controller_400.c - RemoteOps Controller (client) | Reg No: IT24103400 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_PORT 9410
#define MAX_LINE     1024
#define IN_BUF       65536      /* large: LISTPROC replies can be long */

typedef struct {
    int    fd;
    char   buf[IN_BUF];
    size_t len;
} conn_t;

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

/* Same framing idea as the Agent: returns 1 = line, 0 = closed, -1 = error */
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

    char input[MAX_LINE], resp[IN_BUF];

    for (;;) {
        printf("remoteops> ");
        fflush(stdout);
        if (!fgets(input, sizeof input, stdin)) break;      /* Ctrl+D */
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0] == '\0') continue;

        char out[MAX_LINE + 2];
        int n = snprintf(out, sizeof out, "%s\n", input);
        if (send_all(c.fd, out, (size_t)n) < 0) {
            printf("Send failed: connection lost\n");
            break;
        }

        int r = read_line(&c, resp, sizeof resp);
        if (r <= 0) { printf("Connection closed by Agent\n"); break; }
        printf("%s\n", resp);

        if (strncmp(resp, "OK BYE", 6) == 0) break;
    }

    close(c.fd);
    return 0;
}
