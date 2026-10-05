# RemoteOps - IE3090 Network Programming Assignment

Registration number: **IT24103400**

## Personalised values
| Item | Value |
|---|---|
| Agent port | 7000 + 2410 = 9410 |
| Source files | agent_400.c, controller_400.c, Makefile_400 |
| SID tag | last four digits 3400 reversed = SID:0043 |
| Auth token | OPS-3400 |
| Log file | remoteops_IT24103400.log |
| Storage path | ./agentfiles/IT24103400/<filename> |

## Build
    make -f Makefile_400

## Run
    ./agent_400                      # terminal 1
    ./controller_400 127.0.0.1 9410  # terminal 2
