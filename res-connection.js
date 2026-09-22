import Redis from 'ioredis'

function createRedisConnection(){
    return new Redis({
    host:'localhost',
    port:6379,
});

}
export const redis=createRedisConnection();
export const publish=createRedisConnection();
export const subscribe=createRedisConnection();