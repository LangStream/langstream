package ai.langstream.cli.util.git;

import ai.langstream.cli.util.GitClient;
import java.io.IOException;
import java.nio.file.Path;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;

public class JGitClient implements GitClient {


    @Override
    public void cloneRepository(Path directory, String uri, String branch) throws IOException {
        try {
            Git.cloneRepository()
                    .setURI(uri)
                    .setDirectory(directory.toFile())
                    .setBranch(branch)
                    .setDepth(1)
                    .call();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String updateRepository(Path directory, String branch) throws IOException {
        try (final Git open = Git.open(directory.toFile());) {
            open.fetch()
                    .setRefSpecs(new RefSpec("refs/heads/" + branch))
                    .call();
            open.reset()
                    .setMode(ResetCommand.ResetType.HARD)
                    .setRef("origin/" + branch)
                    .call();
            final RevCommit revCommit = open.log().setMaxCount(1).call().iterator().next();
            return revCommit.getId().abbreviate(8).name();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
