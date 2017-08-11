package org.seckill.service.impl;

import org.apache.commons.collections.MapUtils;
import org.seckill.dao.SeckillDao;
import org.seckill.dao.SuccessKilledDao;
import org.seckill.dao.cache.RedisDao;
import org.seckill.dto.Exposer;
import org.seckill.dto.SeckillExecution;
import org.seckill.entity.Seckill;
import org.seckill.entity.SuccessKilled;
import org.seckill.enums.SeckillStatEnum;
import org.seckill.exception.RepeatKillException;
import org.seckill.exception.SeckillCloseException;
import org.seckill.exception.SeckillException;
import org.seckill.service.interfaces.SeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.DigestUtils;

import javax.annotation.Resource;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 徐豪 on 2017/7/28/028.
 */
@Service("seckillService")
public class SeckillServiceImpl implements SeckillService {

    //日志对象
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SeckillDao seckillDao;

    @Autowired
    private RedisDao redisDao;

    @Autowired
    private SuccessKilledDao successKilledDao;

    private final String salt = "wadgydg&d9*(*2q5~war3waef";

    @Override
    public List<Seckill> getSeckillList() {
        return seckillDao.queryAll(0, 4);
    }

    @Override
    public Seckill getById(long seckillId) {
        return seckillDao.queryById(seckillId);
    }

    @Override
    public Exposer exportSeckillUrl(long seckillId) {
        //优化点：缓存优化
        // 1.访问redis
        Seckill seckill = redisDao.getSeckill(seckillId);
        if (seckill == null){
            // 2.访问数据库，根据id查询到Seckill
            seckill = seckillDao.queryById(seckillId);
            if (seckill == null){
                //没有该秒杀
                return new Exposer(false, seckillId);
            } else {
                //3.写入redis
                redisDao.pubSeckill(seckill);
            }
        }

        //秒杀尚未开始或者秒杀已经结束
        Date startTime = seckill.getStartTime();
        Date endTime = seckill.getEndTime();
        Date now = new Date();
        if(now.getTime() < startTime.getTime() || now.getTime() > endTime.getTime()){
            return new Exposer(false, seckillId, now.getTime(), startTime.getTime(),endTime.getTime());
        }


        String md5 = getMd5(seckillId);
        return new Exposer(true, md5, seckillId);
    }

    private String getMd5(long seckillId){
        String base = seckillId + "/" + salt;
        String md5 = DigestUtils.md5DigestAsHex(base.getBytes());
        return md5;
    }

    @Override
    @Transactional
    /**
     * 使用注解控制事务方法的优点：
     * 1. 开发团队达成一致约定，明确标注事务方法的编程风格。
     * 2. 保证事务方法的执行时间尽可能短，不要穿插其他网络操作RPC/HTTP请求或者剥离到事务方法外部
     * 3. 不是所有的方法都需要事务，如只有一条修改操作，只读操作不需要事务控制。
     */
    public SeckillExecution executeSeckill(long seckillId, long userPhone, String md5) throws SeckillException, RepeatKillException, SeckillCloseException {

        //如果没有传递md5值或者md5值不对，表示数据被篡改
        if(md5 == null || !md5.equals(getMd5(seckillId))){
            throw new SeckillException("seckill data rewrite");
        }

        //执行秒杀逻辑：减库存 + 购买行为
        Date nowTime = new Date();

        try {
            /**
             * 秒杀关键：在减库存这条sql语句，在没有执行完之前，事务一直持有行级锁
             * 别的事务只能等待事务提交或者回滚之后拿到这个行级锁
             * 所以通过缩短事务持久减库存这条行级锁的时间可以优化
             * 解决：先插入购买行为，如果是重复秒杀直接事务回滚，不用继续执行减库存这条sql从而不用持有
             * 这条高并发的行级锁，如果不是重复秒杀再执行减库存的语句，这样就少了一步持有行级锁的时间了
             */
            //减库存
            /*int updateCount = seckillDao.reduceNumber(seckillId, nowTime);
            if(updateCount < 0){
                //没有更新到记录，秒杀结束
                throw new SeckillCloseException("seckill is closed");
            } else {
                //记录购买行为
                int insertCount = successKilledDao.insertSuccessKilled(seckillId, userPhone);
                //唯一：seckillId, userPhone
                if(insertCount <= 0){
                    //重复秒杀
                    throw new RepeatKillException("seckill repeated");
                } else {
                    //秒杀成功
                    SuccessKilled successKilled = successKilledDao.queryByIdWithSeckill(seckillId, userPhone);
                    return new SeckillExecution(seckillId, SeckillStatEnum.SUCCESS, successKilled);
                }
            }*/
            //记录购买行为
            int insertCount = successKilledDao.insertSuccessKilled(seckillId, userPhone);
            //唯一：seckillId, userPhone
            if(insertCount <= 0){
                //重复秒杀，事务直接回滚
                throw new RepeatKillException("seckill repeated");
            } else {
                SuccessKilled successKilled = successKilledDao.queryByIdWithSeckill(seckillId, userPhone);
                //减库存，热点商品竞争，执行该sql语句时事务会同时开启行级锁，其他事务不能再对该表进行操作
                int updateCount = seckillDao.reduceNumber(seckillId, nowTime);
                if(updateCount <= 0){
                    //没有更新到记录，秒杀结束，事务回滚，之前的插入记录也被回滚
                    throw new SeckillCloseException("seckill is closed");
                } else {
                    //秒杀成功，事务提交
                    return new SeckillExecution(seckillId, SeckillStatEnum.SUCCESS, successKilled);
                }
            }
        } catch (SeckillCloseException e1){
            throw e1;
        } catch (RepeatKillException e2){
            throw e2;
        } catch (Exception e){
            logger.error(e.getMessage(), e);
            //所有编译期异常转换为运行期异常
            throw new SeckillException("seckill inner error :" + e.getMessage());
        }
    }

    @Override
    public SeckillExecution executeSeckillByRedis(long seckillId, long userPhone, String md5) {

        //如果没有传递md5值或者md5值不对，表示数据被篡改
        if(md5 == null || !md5.equals(getMd5(seckillId))){
            throw new SeckillException("seckill data rewrite");
        }

        //判断商品库存队列是否初始化
        if ( !redisDao.goodsQueueExists(seckillId) ){
            Seckill seckill = seckillDao.queryById(seckillId);
            //获得商品的库存
            int number = seckill.getNumber();
            //获得秒杀的结束时间
            long endTime = seckill.getEndTime().getTime();
            //初始化库存队列
            redisDao.initGoodsQueue(seckillId, number);
            //开启线程处理等待队列中的消息
            new Thread(new Runnable() {
                @Override
                public void run() {
                    System.err.println("线程启动...");
                    //商品库存为0或者秒杀结束时间小于当前时间，就停止，表示秒杀结束
                    while (redisDao.getGoodsQueueNumber(seckillId) > 0 || endTime < new Date().getTime()){

                        String user = redisDao.popWaitQueue(seckillId);
                        //返回nil表示栈顶为空,当栈顶不为空时并且user不等于null时
                        if ( user != null && !"nil".equals(user) && !"null".equals(user) ){
                            //进行秒杀行为：减库存 + 购买行为
                            int result = seckillDao.reduceNumber(seckillId, new Date());
                            if (result > 0){
                                //返回的user字符串为 user:(userPhone)
                                String userPhone = user.split(":")[1];
                                //插入购买记录
                                int m = successKilledDao.insertSuccessKilled(seckillId, Long.parseLong(userPhone));
                                if (m > 0){
                                    //将秒杀成功结果放到结果队列中
                                    redisDao.pushUserToResultsQueue(seckillId, user);
                                }
                            }
                        } else {
                            //栈顶为空时，线程睡眠1毫秒
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    System.err.println("线程结束...");
                }
            }).start();
        }

        //判断结果队列有没有该用户，防止用户重复秒杀
        if (redisDao.userExistsResultsQueue(seckillId, userPhone)){
            return new SeckillExecution(seckillId, SeckillStatEnum.REPEAT_KILL);
        }

        /**
         * 判断当前库存，>0则将用户放入排队队列，<=0则返回秒杀失败，
         * 使用lpop保证操作的原子性，防止多线程出现llen判断>0，临界值的时候出现多个线程挤进来
         * 最后导致超卖的情况
         */
        if ( !(redisDao.pushWaitQueue(seckillId, userPhone))){
            return new SeckillExecution(seckillId, SeckillStatEnum.END);
        }

        //SuccessKilled successKilled = successKilledDao.queryByIdWithSeckill(seckillId, userPhone);
        //此处不做数据库是否更新完成，忽略系统异常产生的结果
        return new SeckillExecution(seckillId, SeckillStatEnum.SUCCESS, null);
    }

    /**
     * 实现效果没有redis好
     * @param seckillId
     * @param userPhone
     * @param md5
     * @return
     */
    @Override
    public SeckillExecution executeSeckillByActiveMQ(long seckillId, long userPhone, String md5) {

        //如果没有传递md5值或者md5值不对，表示数据被篡改
        /*if(md5 == null || !md5.equals(getMd5(seckillId))){
            throw new SeckillException("seckill data rewrite");
        }*/

        return null;
    }

    @Override
    public SeckillExecution executeSeckillProcedure(long seckillId, long userPhone, String md5) {
        if (md5 == null || !md5.equals(getMd5(seckillId))){
            return new SeckillExecution(seckillId,SeckillStatEnum.DATA_REWRITE);
        }
        Date killTime = new Date();
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("seckillId", seckillId);
        map.put("userPhone", userPhone);
        map.put("killTime", killTime);
        map.put("result", null);
        //执行存储过程，result被赋值
        try{
            seckillDao.killPyProcedure(map);
            //获取result
            int result = MapUtils.getInteger(map, "result", -2);
            if (result == 1){
                SuccessKilled sk = successKilledDao.
                        queryByIdWithSeckill(seckillId, userPhone);
                return new SeckillExecution(seckillId,SeckillStatEnum.SUCCESS,sk);
            } else {
                return new SeckillExecution(seckillId, SeckillStatEnum.stateOf(result));
            }
        } catch(Exception e){
            logger.error(e.getMessage(),e);
            return new SeckillExecution(seckillId, SeckillStatEnum.INNER_ERROR);
        }
    }
}
