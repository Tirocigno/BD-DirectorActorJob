package it.unibo.bd1819.utils;

/**
 * Utility class which contains all the input/output paths
 */
public class Paths {

    public static String GENERIC_INPUT_PATH = "bigdata/dataset/";
    public static String GENERIC_OUTPUT_PATH = "mapreduce/";
    public static String NAME_BASICS_PATH = GENERIC_INPUT_PATH + "namebasics";
    public static String TITLE_PRINCIPALS_PATH = GENERIC_INPUT_PATH + "principals";
    public static String TITLE_BASICS_PATH = GENERIC_INPUT_PATH + "titlebasics";
    public static String JOIN_TITLE_BASICS_PRINCIPALS_PATH = GENERIC_OUTPUT_PATH + "jointitlebasicsprincipals";
    public static String AGGREGATED_DIRECTORS_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "aggregateddirectors";
    public static String JOIN_ACTORS_DIRECTORS_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "joinactorsdirector";
    public static String THREE_ACTORS_DIRECTORS_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "threeactorsdirector";
    public static String JOIN_DIRECTORS_NAME_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "joindirectorsname";
    public static String MAIN_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "output";

}
