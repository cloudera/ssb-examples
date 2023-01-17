import org.apache.flink.table.functions.ScalarFunction;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;

public class LookupAddress extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    public transient static final Faker FAKER = new Faker();
    public transient static final Address ADDRESS = FAKER.address();

    public String eval(String longitude, String latitude) {
        return ADDRESS.fullAddress();
    }
}
