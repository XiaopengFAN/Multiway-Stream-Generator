import java.security.Key;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DSTuple {
    private DSModel[] models;
    private String primaryKey="";

    DSTuple (DSModel[] models) {
        this.models = models;
    }

    public void setPrimaryKey(String primaryKey){
//        Set the primary key of this tuple that will shown first.
        this.primaryKey = primaryKey;
    }

    public String produceTuple() {
//        Generate a tuple
        Map<String, String> tpmap = new HashMap<>();
        for (DSModel model : models) {
            if (model.getMd() == "Poisson" ||
                    model.getMd() == "Zipf"||
                    model.getMd() == "UserDefineInt"||
                    model.getMd() == "UniformInt") {
                tpmap.put(model.getKey(), Integer.toString(model.startRamdomInt()));
            } else if (model.getMd() == "Gaussian"      ||
                    model.getMd() == "UniformDouble"    ||
                    model.getMd() == "Exponential") {
                tpmap.put(model.getKey(), Double.toString(model.startRamdomDouble()));
            }
        }

//        Output this tuple
        String text = "";
//        Check if we have the Key-Property that should be print out first.
        if (tpmap.containsKey(primaryKey)){
//            text += (primaryKey + "  " + tpmap.get(primaryKey) +  "  " );     // Option 1
            text += (tpmap.get(primaryKey) + ",");                              // Option 2

            tpmap.remove(primaryKey);
        }
//        Then we should print tuple's time stamp.
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String time = df.format(new Date());                  // Option 1
//        text += ("Time" + "  " + time +  "  " );
        String time = String.valueOf(new Date().getTime());     // Option 2
        text += (time + ",");

//        Finally the other properties
        for (Map.Entry<String, String> entry : tpmap.entrySet()) {
//            text += (entry.getKey() +  "  " + entry.getValue() + "  ");   // Option 1
            text += (entry.getValue() + ",");                               // Option 2
        }
//        Here we should delete the last ','  (e.g. ,translate "1,2,3," to "1,2,3").
        text = text.substring(0,text.length()-1);

//        Finally return this tuple as String
        System.out.println(text);
        return (text+"\n");
    }
}
