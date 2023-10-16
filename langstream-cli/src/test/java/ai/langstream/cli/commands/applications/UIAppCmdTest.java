package ai.langstream.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class UIAppCmdTest {

    @Test
    void openBrowser() {
        // ok
        UIAppCmd.openBrowserAtPort("echo", 9029);
        // fail
        UIAppCmd.openBrowserAtPort("fail", 9029);
    }
}