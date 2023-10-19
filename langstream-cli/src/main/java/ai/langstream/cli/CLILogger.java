package ai.langstream.cli;

public interface CLILogger {
    void log(Object message);

    void error(Object message);

    boolean isDebugEnabled();

    void debug(Object message);


    class SystemCliLogger implements CLILogger {
        @Override
        public void log(Object message) {
            System.out.println(message);

        }

        @Override
        public void error(Object message) {
            System.err.println(message);
        }

        @Override
        public boolean isDebugEnabled() {
            return true;
        }

        @Override
        public void debug(Object message) {
            log(message);
        }
    }

}
