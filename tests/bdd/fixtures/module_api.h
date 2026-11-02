/* Minimal independently written declarations of public Redis Modules ABI v1.
 * No Redis implementation or copied Redis header is included. */
#ifndef FFKV_FIXTURE_MODULE_API_H
#define FFKV_FIXTURE_MODULE_API_H
#include <stddef.h>
#include <stdint.h>
typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleString RedisModuleString;
typedef struct RedisModuleCallReply RedisModuleCallReply;
typedef struct RedisModuleCommand RedisModuleCommand;
typedef struct RedisModuleKey RedisModuleKey;
typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *,RedisModuleString **,int);
typedef int (*GetApiFn)(const char *,void *);
typedef struct {int version;size_t history_size,key_size,arg_size;} InfoVersion;
typedef struct {const InfoVersion *version;const char *summary,*complexity,*since;const void *history;const char *tips;int arity;const void *keys,*args;} CommandInfo;
#define API(ret,name,args) static ret (*RedisModule_##name) args
API(void,SetModuleAttribs,(RedisModuleCtx *,const char *,int,int));
API(int,IsModuleNameBusy,(const char *));
API(int,CreateCommand,(RedisModuleCtx *,const char *,RedisModuleCmdFunc,const char *,int,int,int));
API(RedisModuleCommand *,GetCommand,(RedisModuleCtx *,const char *));
API(int,SetCommandInfo,(RedisModuleCommand *,const CommandInfo *));
API(RedisModuleString *,CreateString,(RedisModuleCtx *,const char *,size_t));
API(void,FreeString,(RedisModuleCtx *,RedisModuleString *));
API(void,RetainString,(RedisModuleCtx *,RedisModuleString *));
API(const char *,StringPtrLen,(const RedisModuleString *,size_t *));
API(int,StringToLongLong,(const RedisModuleString *,long long *));
API(int,WrongArity,(RedisModuleCtx *));
API(int,ReplyWithLongLong,(RedisModuleCtx *,long long));
API(int,ReplyWithStringBuffer,(RedisModuleCtx *,const char *,size_t));
API(int,ReplyWithString,(RedisModuleCtx *,RedisModuleString *));
API(int,ReplyWithArray,(RedisModuleCtx *,long long));
API(int,ReplyWithError,(RedisModuleCtx *,const char *));
API(int,ReplyWithSimpleString,(RedisModuleCtx *,const char *));
API(RedisModuleCallReply *,Call,(RedisModuleCtx *,const char *,const char *,...));
API(int,ReplyWithCallReply,(RedisModuleCtx *,RedisModuleCallReply *));
API(void,FreeCallReply,(RedisModuleCallReply *));
API(RedisModuleKey *,OpenKey,(RedisModuleCtx *,RedisModuleString *,int));
API(void,CloseKey,(RedisModuleKey *));
API(int,StringSet,(RedisModuleKey *,RedisModuleString *));
typedef RedisModuleString *(*ConfigGet)(const char *,void *);
typedef int (*ConfigSet)(const char *,RedisModuleString *,void *,RedisModuleString **);
typedef int (*ConfigApply)(RedisModuleCtx *,void *,RedisModuleString **);
API(int,RegisterStringConfig,(RedisModuleCtx *,const char *,const char *,unsigned,ConfigGet,ConfigSet,ConfigApply,void *));
API(int,LoadConfigs,(RedisModuleCtx *));
#undef API
#define GET_API(name) do {if (get_api("RedisModule_" #name,&RedisModule_##name)) return 1;} while (0)
static int RedisModule_Init(RedisModuleCtx *ctx,const char *name,int version,int api) {
    GetApiFn get_api=*(GetApiFn *)(void *)ctx;
    GET_API(SetModuleAttribs);GET_API(IsModuleNameBusy);GET_API(CreateCommand);
    GET_API(GetCommand);GET_API(SetCommandInfo);GET_API(CreateString);GET_API(FreeString);GET_API(RetainString);
    GET_API(StringPtrLen);GET_API(StringToLongLong);GET_API(WrongArity);GET_API(ReplyWithLongLong);
    GET_API(ReplyWithStringBuffer);GET_API(ReplyWithString);GET_API(ReplyWithArray);GET_API(ReplyWithError);GET_API(ReplyWithSimpleString);
    GET_API(Call);GET_API(ReplyWithCallReply);GET_API(FreeCallReply);GET_API(OpenKey);GET_API(CloseKey);GET_API(StringSet);
    GET_API(RegisterStringConfig);GET_API(LoadConfigs);
    if (api!=1||RedisModule_IsModuleNameBusy(name)) return 1;
    RedisModule_SetModuleAttribs(ctx,name,version,api);return 0;
}
#endif
