package it.unibo.bd1819.mocktest;


public class MockMain {

    private final static String ROLE = "director";
    private final static String VALUE_SEPARATOR = "\\t";
    private final static String EMPTY_VALUE = "";

    private static String filterByRole(final String text) {
        String line = text;
        String[] values = line.split(VALUE_SEPARATOR);
        if(values[3].equals(ROLE)) return values[2];
        else return "";
    }
    public static void main(String[] args) throws Exception {
        String text = "tt0000001\t1\tnm1588970\tself\t\\N\t[\"Herself\"]\n" +
                "tt0000001\t2\tnm0005690\tdirector\t\\N\t\\N\n" +
                "tt0000001\t3\tnm0374658\tcinematographer\tdirector of photography\t\\N\n" +
                "tt0000002\t1\tnm0721526\tdirector\t\\N\t\\N\n" +
                "tt0000002\t2\tnm1335271\tcomposer\t\\N\t\\N\n" +
                "tt0000003\t1\tnm0721526\tdirector\t\\N\t\\N\n" +
                "tt0000003\t2\tnm5442194\tproducer\tproducer\t\\N\n" +
                "tt0000003\t3\tnm1335271\tcomposer\t\\N\t\\N\n" +
                "tt0000003\t4\tnm5442200\teditor\t\\N\t\\N\n" +
                "tt0000004\t1\tnm0721526\tdirector\t\\N\t\\N\n" +
                "tt0000004\t2\tnm1335271\tcomposer\t\\N\t\\N\n";

        for(String line:text.split("\\n")) {
            if(!filterByRole(line).equals(""))
            System.out.println(filterByRole(line));
        }
    }
}
