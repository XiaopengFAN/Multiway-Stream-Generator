package Islotus;

import java.util.Random;

public class ConfClass {
    private int poolSize;
    private int groupNum;
    private int[][] threadConf;

    private int sleepTimeGps;  //millisecond
    private int sleepTimeMeter;  //millisecond

    private int aliveTime;  //minute

    public ConfClass() {
        try {
            this.poolSize = 10;
            this.groupNum = 2;
            this.threadConf = new int[poolSize][3];
            this.sleepTimeGps = 1000;  //milliSeconds
            this.sleepTimeMeter = 1000;  //milliSeconds
            this.aliveTime = 10;  //seconds
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public ConfClass(int poolSize, int groupNum, int sleepTimeGps, int sleepTimeMeter, int aliveTime) {
        try {
            this.poolSize = poolSize;
            this.groupNum = groupNum;
            this.threadConf = new int[poolSize][3];
            this.sleepTimeGps = sleepTimeGps;
            this.sleepTimeMeter = sleepTimeMeter;
            this.aliveTime = aliveTime; //thread alive time, 0 is forever, another is limited time
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void setPoolSize(int poolSize){
        try {
            this.poolSize = poolSize;
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public int getPoolSize(){
        return this.poolSize;
    }

    public String getPlateId(int length) {
        StringBuilder val = new StringBuilder("粤B");
        Random random = new Random();

        //参数length，表示生成几位随机数
        for(int i=0; i<length; ++i) {
            String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
            //输出字母还是数字
            if("char".equalsIgnoreCase(charOrNum) ) {
                //输出是大写字母还是小写字母
                //int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
                val.append((char) (random.nextInt(26) + 65));
            } else {
                val.append(String.valueOf(random.nextInt(10)));
            }
        }
        return val.toString();
    }

    private int setConfGroupNum(int poolSize, int groupNum, int lineNum){
        int result = -1;
        try{
            int groupSize = (int)Math.ceil((double)poolSize / groupNum);
            result = lineNum / groupSize;
        } catch(Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public void setThreadConf() {
        try {
            if (sleepTimeGps < 0 || sleepTimeMeter < 0 || aliveTime< 0) {
                throw new IllegalArgumentException();
            }
            if(poolSize <= 0 || groupNum <= 0){
                System.out.print("poolSize and groub must be big than zero!");
                throw new Exception("poolSize and groub must be big than zero!");
            }
            if(groupNum > poolSize) {
                System.out.println("groupNum is big thran poolsize!");
                throw new Exception("poolSize and groub must be big than zero!");
            }
            for (int i = 0; i < poolSize; ++i) {
                threadConf[i][0] = sleepTimeGps;  //thread sleep time is 600
                threadConf[i][1] = sleepTimeMeter;
                threadConf[i][2] = setConfGroupNum(poolSize,groupNum,i);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public int[][] getThreadConf(){
        return this.threadConf;
    }
    public void setSleepTimeGps(int sleepTimeGps) {
        try {
            if (sleepTimeGps < 0) {
                throw new IllegalArgumentException();
            }
            this.sleepTimeGps = sleepTimeGps;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public int getSleepTimeGps() {
        //System.out.println(this.sleepTimeGps);
        return this.sleepTimeGps;
    }
    public void setSleepTimeMeter(int sleepTimeMeter) {
        try {
            if (sleepTimeMeter < 0) {
                throw new IllegalArgumentException();
            }
            this.sleepTimeMeter = sleepTimeMeter;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public int getSleepTimeMeter() {
        //System.out.println(this.sleepTimeMeter);
        return this.sleepTimeMeter;
    }
    public void setAliveTime(int aliveTime) {
        try {
            if (aliveTime < 0) {
                throw new IllegalArgumentException();
            }
            this.aliveTime = aliveTime;
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public int getAliveTime() {
        //System.out.println(this.aliveTime);
        return this.aliveTime;
    }

    public void printThreadConf() {
        try {
            for (int i = 0; i < poolSize; ++i) {
                System.out.print(threadConf[i][0] + " " + threadConf[i][1]);
                System.out.println();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}