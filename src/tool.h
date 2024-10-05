//
// Created by workshop on 3/8/2022.
//

#ifndef HYBRIDMM_TOOL_H
#define HYBRIDMM_TOOL_H

#include <fcntl.h>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <cstdlib>

typedef enum IndexType{
    CLHT,
    LFHT
}IndexType;

typedef enum LOGLevel{
    DEBUG,
    LOG,
    WARN,
    ERROR
}LOGLevel;
#define MAX_LOG_SIZE 1000
#define LOG_PATH "log"
extern thread_local int threadId;
inline void vmlog(LOGLevel logLevel,const char* fmat, ...);

void vmlog(LOGLevel logLevel, const char* fmat, ...){
    //    if(logLevel == InsertLog){
    //        return;
    //    }
    if(logLevel < WARN){
        return;
    }
    //get the string passed by the caller through the format string
    va_list argptr;
    va_start(argptr, fmat);
    char buffer[MAX_LOG_SIZE]="\0";
    int count = vsprintf(buffer, fmat, argptr);
    va_end(argptr);
    if(count >= MAX_LOG_SIZE)
    {
        printf("ERROR:log message too many\n");
        return;
    }
    static int logF = -1;
    if(logF <= 0)
    {
        if (access(LOG_PATH,0) == 0 ){
            remove(LOG_PATH);
        }
        logF = open(LOG_PATH, O_WRONLY|O_CREAT, S_IRWXU  );
        lseek(logF, 0, SEEK_SET);
    }
    char output[MAX_LOG_SIZE + 100];
    snprintf(output, sizeof(output),"(pid:%d)---%s\n",threadId, buffer);
    write(logF, output, strlen(output));
    if(logLevel == ERROR){
        printf("!!!!!!!!!!ERROR:%s", output);
        exit(-1);
    }
}
#endif //HYBRIDMM_TOOL_H
