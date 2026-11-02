/* Real native library fixture: exported lifecycle functions, public ABI only. */
#include "module_api.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
static RedisModuleString *prefix;
static RedisModuleString **initial;
static int initial_count;
static long long counter;
static int refuse_unload;
static char *unload_marker;
static RedisModuleString *copy_string(RedisModuleString *value) {size_t n;const char *p=RedisModule_StringPtrLen(value,&n);return RedisModule_CreateString(NULL,p,n);}
static RedisModuleString *get_prefix(const char *name,void *data) {(void)name;(void)data;return prefix;}
static int set_prefix(const char *name,RedisModuleString *value,void *data,RedisModuleString **error) {
    (void)name;(void)data;(void)error;
    if (prefix) RedisModule_FreeString(NULL,prefix);
    prefix=copy_string(value);return prefix ? 0:1;
}
static int echo(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if (argc!=2) return RedisModule_WrongArity(ctx);
    size_t a,b;const char *p=RedisModule_StringPtrLen(prefix,&a),*v=RedisModule_StringPtrLen(argv[1],&b);
    if (a>SIZE_MAX-b) return RedisModule_ReplyWithError(ctx,"ERR too large");
    char *joined=malloc(a+b+1);if (!joined) return RedisModule_ReplyWithError(ctx,"ERR allocation failed");
    memcpy(joined,p,a);memcpy(joined+a,v,b);int rc=RedisModule_ReplyWithStringBuffer(ctx,joined,a+b);free(joined);return rc;
}
static int sum(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if (argc<2) return RedisModule_WrongArity(ctx);long long total=0;
    for (int i=1;i<argc;++i) {long long n;if (RedisModule_StringToLongLong(argv[i],&n)|| (n>0&&total>LLONG_MAX-n)||(n<0&&total<LLONG_MIN-n)) return RedisModule_ReplyWithError(ctx,"ERR invalid integer or overflow");total+=n;}
    return RedisModule_ReplyWithLongLong(ctx,total);
}
static int count(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {(void)argv;if(argc!=1)return RedisModule_WrongArity(ctx);return RedisModule_ReplyWithLongLong(ctx,++counter);}
static int initialization(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {(void)argv;if(argc!=1)return RedisModule_WrongArity(ctx);RedisModule_ReplyWithArray(ctx,initial_count);for(int i=0;i<initial_count;++i)RedisModule_ReplyWithString(ctx,initial[i]);return 0;}
static int fail(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {(void)argv;(void)argc;return RedisModule_ReplyWithError(ctx,"EXTENSION deliberately returned an error");}
static int get(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if(argc!=2)return RedisModule_WrongArity(ctx);RedisModuleCallReply *reply=RedisModule_Call(ctx,"GET","sE",argv[1]);if(!reply)return RedisModule_ReplyWithError(ctx,"ERR Call failed");int rc=RedisModule_ReplyWithCallReply(ctx,reply);RedisModule_FreeCallReply(reply);return rc;
}
static int incr(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if(argc!=2)return RedisModule_WrongArity(ctx);RedisModuleCallReply *reply=RedisModule_Call(ctx,"INCR","s!E",argv[1]);if(!reply)return RedisModule_ReplyWithError(ctx,"ERR Call failed");int rc=RedisModule_ReplyWithCallReply(ctx,reply);RedisModule_FreeCallReply(reply);return rc;
}
static int keyset(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if(argc!=3)return RedisModule_WrongArity(ctx);RedisModuleKey *key=RedisModule_OpenKey(ctx,argv[1],3);if(!key)return RedisModule_ReplyWithError(ctx,"ERR OpenKey failed");int rc=RedisModule_StringSet(key,argv[2]);RedisModule_CloseKey(key);return rc ? RedisModule_ReplyWithError(ctx,"ERR StringSet failed"):RedisModule_ReplyWithSimpleString(ctx,"OK");
}
static int unload_control(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {long long n;if(argc!=2||RedisModule_StringToLongLong(argv[1],&n))return RedisModule_WrongArity(ctx);refuse_unload=n!=0;return RedisModule_ReplyWithSimpleString(ctx,"OK");}
static int hold(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if(argc!=3)return RedisModule_WrongArity(ctx);size_t n;const char *path=RedisModule_StringPtrLen(argv[1],&n);char *name=malloc(n+1);if(!name)return 1;memcpy(name,path,n);name[n]=0;
    FILE *file=fopen(name,"wb");free(name);if(!file)return RedisModule_ReplyWithError(ctx,"ERR signal failed");fputs("active",file);fclose(file);
    long long ms;if(RedisModule_StringToLongLong(argv[2],&ms)||ms<0||ms>10000)return RedisModule_ReplyWithError(ctx,"ERR invalid delay");struct timespec delay={ms/1000,(ms%1000)*1000000};nanosleep(&delay,NULL);return RedisModule_ReplyWithSimpleString(ctx,"OK");
}
static int register_command(RedisModuleCtx *ctx,const char *name,RedisModuleCmdFunc callback,const char *flags,int arity,int first,int last,int step) {
    if(RedisModule_CreateCommand(ctx,name,callback,flags,first,last,step))return 1;
    static const InfoVersion version={1,0,0,0};CommandInfo info={0};info.version=&version;info.arity=arity;
    return RedisModule_SetCommandInfo(RedisModule_GetCommand(ctx,name),&info);
}
int RedisModule_OnLoad(RedisModuleCtx *ctx,RedisModuleString **argv,int argc) {
    if(RedisModule_Init(ctx,"fixture",10000,1))return 1;
    /* Failure happens after a real partial registration: it must never leak. */
    if(argc) {size_t n;const char *arg=RedisModule_StringPtrLen(argv[0],&n);
        if(n==14&&!memcmp(arg,"--partial-fail",14)) {RedisModule_CreateCommand(ctx,"fixture.echo",echo,"readonly",0,0,0);return 1;}
        if(n==13&&!memcmp(arg,"--unknown-api",13)) {void *api=NULL;GetApiFn lookup=*(GetApiFn *)(void *)ctx;return lookup("RedisModule_UnsupportedFixtureAPI",&api)==0 ? 0:1;}
    }
    if(RedisModule_RegisterStringConfig(ctx,"prefix","",1,get_prefix,set_prefix,NULL,NULL)||RedisModule_LoadConfigs(ctx))return 1;
    initial_count=argc;initial=calloc((size_t)argc,sizeof(*initial));if(argc&&!initial)return 1;
    for(int i=0;i<argc;++i)initial[i]=copy_string(argv[i]);
    if(argc>=2) {size_t n;const char *arg=RedisModule_StringPtrLen(argv[0],&n);if(n==15&&!memcmp(arg,"--unload-marker",15)) {const char *p=RedisModule_StringPtrLen(argv[1],&n);unload_marker=malloc(n+1);if(!unload_marker)return 1;memcpy(unload_marker,p,n);unload_marker[n]=0;}}
    return register_command(ctx,"fixture.echo",echo,"readonly admin",2,0,0,0)||register_command(ctx,"fixture.sum",sum,"readonly fast",-2,0,0,0)||register_command(ctx,"fixture.counter",count,"write",1,0,0,0)||register_command(ctx,"fixture.initialization",initialization,"readonly",1,0,0,0)||register_command(ctx,"fixture.fail",fail,"readonly",1,0,0,0)||register_command(ctx,"fixture.get",get,"readonly",2,1,1,1)||register_command(ctx,"fixture.incr",incr,"write",2,1,1,1)||register_command(ctx,"fixture.keyset",keyset,"write deny-oom",3,1,1,1)||register_command(ctx,"fixture.unload",unload_control,"admin",2,0,0,0)||register_command(ctx,"fixture.hold",hold,"admin noscript",3,0,0,0);
}
int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    (void)ctx;if(refuse_unload)return 1;
    if(unload_marker) {FILE *f=fopen(unload_marker,"wb");if(!f)return 1;fputs("unloaded",f);fclose(f);free(unload_marker);unload_marker=NULL;}
    for(int i=0;i<initial_count;++i)RedisModule_FreeString(NULL,initial[i]);free(initial);initial=NULL;initial_count=0;
    if(prefix)RedisModule_FreeString(NULL,prefix);prefix=NULL;return 0;
}
