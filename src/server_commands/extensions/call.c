/* Independent adapter for the public RedisModule_Call varargs ABI. Native
 * modules still enter through GetApi and RedisModule_OnLoad, not a wire shim. */
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleString RedisModuleString;
typedef struct RedisModuleCallReply RedisModuleCallReply;
typedef struct { const char *data; size_t len; } FfkvCallArg;
extern RedisModuleCallReply *ffkv_module_callv(RedisModuleCtx *, const char *, const FfkvCallArg *, size_t);
extern int ffkv_module_get_api(const char *, void *);
static int append(FfkvCallArg **args, size_t *count, size_t *capacity, const char *p, size_t n) {
    if (*count == *capacity) {
        size_t cap = *capacity ? *capacity * 2 : 8;
        if (cap > SIZE_MAX / sizeof(**args)) return 0;
        void *next = realloc(*args, cap * sizeof(**args));
        if (!next) return 0;
        *args = next; *capacity = cap;
    }
    (*args)[(*count)++] = (FfkvCallArg){p, n}; return 1;
}
RedisModuleCallReply *ffkv_module_call(RedisModuleCtx *ctx, const char *name, const char *format, ...) {
    typedef const char *(*StringPtr)(const RedisModuleString *, size_t *);
    StringPtr string_ptr = NULL;
    if (ffkv_module_get_api("RedisModule_StringPtrLen", &string_ptr)) return NULL;
    FfkvCallArg *args = NULL; size_t count = 0, cap = 0;
    char **owned = NULL; size_t owned_count = 0;
    va_list ap; va_start(ap, format);
    int okay = 1;
    for (const char *f = format; okay && *f; ++f) {
        const char *p; size_t n;
        switch (*f) {
        case 'c': p = va_arg(ap, const char *); okay = p && append(&args,&count,&cap,p,strlen(p)); break;
        case 'b': p = va_arg(ap, const char *); n = va_arg(ap,size_t); okay = append(&args,&count,&cap,p,n); break;
        case 's': { RedisModuleString *s = va_arg(ap,RedisModuleString *); p = string_ptr(s,&n); okay = p && append(&args,&count,&cap,p,n); break; }
        case 'v': { RedisModuleString **values = va_arg(ap,RedisModuleString **); n = va_arg(ap,size_t); for (size_t i=0; okay && i<n; ++i) { size_t len; p=string_ptr(values[i],&len); okay=p && append(&args,&count,&cap,p,len); } break; }
        case 'l': { long long value=va_arg(ap,long long); char *number=malloc(32); if (!number) {okay=0;break;} int len=snprintf(number,32,"%lld",value); char **next=realloc(owned,(owned_count+1)*sizeof(*owned)); if (!next) {free(number);okay=0;break;} owned=next;owned[owned_count++]=number;okay=append(&args,&count,&cap,number,(size_t)len);break; }
        /* All resolved writes already propagate through the atomic engine. E
         * requests an error reply rather than an exception (the native default).
         * Other behavioral modifiers are not silently accepted. */
        case '!': case 'E': break;
        default: okay=0; errno=EINVAL; break;
        }
    }
    va_end(ap);
    RedisModuleCallReply *reply = okay ? ffkv_module_callv(ctx,name,args,count) : NULL;
    for (size_t i=0;i<owned_count;++i) free(owned[i]);
    free(owned);free(args);return reply;
}
