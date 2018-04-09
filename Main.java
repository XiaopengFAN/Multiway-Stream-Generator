package Islotus;

public class Main {
    public static void main(String args[]) {
        int poolSize = 30;
        int groupNum = 3;
        int sleepTimeGps = 2000;  //milliSeconds
        int sleepTimeMeter = 2000;  //milliSeconds sleepTimeMeter >= sleepTimeGps
        int aliveTime = 10;  //seconds

        ConfClass conf = new ConfClass(poolSize,groupNum,sleepTimeGps,sleepTimeMeter,aliveTime);
        conf.setThreadConf();

        ScheduleThreadPool scheduleThreadPool = new ScheduleThreadPool(conf);
        scheduleThreadPool.doTask();
    }
}
