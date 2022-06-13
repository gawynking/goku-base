import com.pgman.goku.config.ConfigurationManager;
import com.pgman.goku.util.DateUtils;
import com.pgman.goku.util.SQLUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class Test {

    @org.junit.Test
    public void testUpsert(){

        String string = ConfigurationManager.getString("broker.list");
        System.out.println(string);

    }




}
