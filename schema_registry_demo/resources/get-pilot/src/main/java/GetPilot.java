import org.apache.flink.table.functions.ScalarFunction;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

import java.sql.Timestamp;

public class GetPilot extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    public transient static final Faker FAKER = new Faker();
    public transient static final Name NAME = FAKER.name();

    public String eval(String aircraft, String airport, Timestamp eventTimestamp) {
        return NAME.fullName();
    }
}
