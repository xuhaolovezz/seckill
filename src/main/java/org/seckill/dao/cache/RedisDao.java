package org.seckill.dao.cache;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.seckill.entity.Seckill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * Created by 徐豪 on 2017/7/29/029.
 */
public class RedisDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    //序列化
    private RuntimeSchema<Seckill> schema = RuntimeSchema.createFrom(Seckill.class);

    private final JedisPool jedisPool;

    public RedisDao(String ip, int port){
        jedisPool = new JedisPool(ip, port);
    }

    public Seckill getSeckill(long seckillId){
        //redis操作逻辑
        try{
            Jedis jedis= jedisPool.getResource();
            try {
                String key = "seckill:" + seckillId;
                //jedis默认没有实现内部序列化操作，所以不能直接放对象
                // 得到->byte[]  -> 反序列化 -> Object(Seckill)
                //采用自定义序列化：protostuff时间快一倍，空间压缩5-10分之一
                byte[] bytes = jedis.get(key.getBytes());
                //如果有缓存
                if (bytes != null){
                    //创建个空对象用于写入反序列化对象
                    Seckill seckill = schema.newMessage();
                    //写入反序列化对象
                    ProtostuffIOUtil.mergeFrom(bytes, seckill, schema);
                    //seckill被反序列化
                    return seckill;
                }
            } finally {
                //无论是否发生异常都关闭jedis
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return null;
    }

    public String pubSeckill(Seckill seckill){
        //set Object(Seckill) -> 序列化 -> byte[]
        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String key = "seckill:"+seckill.getSeckillId();
                //如果对象比较大，使用默认的缓存提高速度
                byte[] bytes = ProtostuffIOUtil.toByteArray(seckill,schema,
                        LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
                //缓存超时的时间
                int timeout = 60 * 60;
                String result = jedis.setex(key.getBytes(), timeout, bytes);
                return result;
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }
        return null;
    }

    /**
     * 判断库存队列是否初始化
     * 库存队列key：goodsQueue:(商品id)
     */
    public boolean goodsQueueExists(long seckillId){

        try {
            Jedis jedis = jedisPool.getResource();
            try{
                if(jedis.exists("goodsQueue:"+seckillId)){
                    return true;
                } else {
                    return false;
                }
            } finally {
                jedis.close();
            }
        } catch (Exception e){
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * 初始化库存队列
     */
    public void initGoodsQueue(long seckillId, int number){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                //往库存队列插入和库存一样数量的值
                for (int i = 0; i <number ; i++) {
                     jedis.lpush("goodsQueue:"+seckillId,String.valueOf(i));
                }
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

    }

    /**
     * 判断用户是否秒杀成功，防止重复秒杀
     * @param userPhone
     * @return
     */
    public boolean userExistsResultsQueue(long seckillId,long userPhone){

        try{
            Jedis jedis = jedisPool.getResource();

            try {
                String resultsQueue = "resultsQueue:" + seckillId;
                String value = "user:" + userPhone;
                //不存在结果队列表示还没有人秒杀成功
                if (!jedis.exists(resultsQueue)){
                    return false;
                }
                //取出所有结果队列
                List<String> resultList = jedis.lrange(resultsQueue, 0, -1);

                //判断当前用户是否在结果队列里
                if (resultList.contains(value)){
                    return true;
                } else {
                    return false;
                }

            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return false;
    }

    /**
     * 获得库存队列的大小
     */
    public long getGoodsQueueNumber(long seckillId){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String goodsQueue = "goodsQueue:" + seckillId;
                return jedis.llen(goodsQueue);
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return 0;
    }

    /**
     * 获得秒杀等待队列的大小
     */
    public long getWaitQueueNumber(long seckillId){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String waitQueue = "waitQueue:" + seckillId;
                return jedis.llen(waitQueue);
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return 0;
    }

    /**
     * 将当前用户放入秒杀排队队列中并且弹出一个库存队列中的值
     */
    public boolean pushWaitQueue(long seckillId, long userPhone){

        try{
            Jedis jedis = jedisPool.getResource();

            try {
                String waitQueue = "waitQueue:" + seckillId;
                String goodsQueue = "goodsQueue:" + seckillId;

                //判断库存队列中是否还有库存
                if ("nil".equals(jedis.lpop(goodsQueue))){
                    //弹出库存队列中的栈顶的值，返回nil表示栈空
                    return false;
                } else {
                    String value = "user:" + userPhone;
                    //有库存将当前用户放入排队队列等待数据库处理
                    jedis.rpush(waitQueue,value);
                    return true;
                }

            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return false;
    }

    /**
     * 将排队队列中的栈顶元素弹出
     */
    public String popWaitQueue(long seckillId){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String waitQueue = "waitQueue:" + seckillId;
                //将当前用户从排队队列中弹出
                String user = jedis.lpop(waitQueue);
                return user;
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return null;
    }

    /**
     * 将用户插入到结果队列中
     * @param seckillId
     * @param user
     * @return
     */
    public long pushUserToResultsQueue(long seckillId, String user){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String resultsQueue = "resultsQueue:" + seckillId;
                long result = jedis.rpush(resultsQueue, user);
                return result;
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return 0;
    }

    /**
     * 将插入失败的记录放入失败队列中
     */
    public long pushUserToFailureQueue(long seckillId, String user){

        try{
            Jedis jedis = jedisPool.getResource();
            try {
                String failureQueue = "failureQueue:" + seckillId;
                long result =jedis.rpush(failureQueue,user);
                return result;
            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }
        return 0;
    }

    /**
     * 判断用户是否秒杀成功
     * @param userPhone
     * @return
     *//*
    public boolean userKillSuccess(long seckillId,long userPhone){

        try{
            Jedis jedis = jedisPool.getResource();

            try {
                String waitQueue = "waitQueue:" + seckillId;
                String value = "user:" + userPhone;
                if (!jedis.exists(waitQueue)){
                    return false;
                }
                //取出所有结果队列
                List<String> resultList = jedis.lrange(waitQueue, 0, -1);

                //判断当前用户是否在等待队列里
                if (resultList.contains(value)){
                    return true;
                } else {
                    return false;
                }

            } finally {
                jedis.close();
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
        }

        return false;
    }*/

}
