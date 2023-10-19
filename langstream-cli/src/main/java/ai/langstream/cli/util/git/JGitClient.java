/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        try (final Git open = Git.open(directory.toFile()); ) {
            open.fetch().setRefSpecs(new RefSpec("refs/heads/" + branch)).call();
            open.reset().setMode(ResetCommand.ResetType.HARD).setRef("origin/" + branch).call();
            final RevCommit revCommit = open.log().setMaxCount(1).call().iterator().next();
            return revCommit.getId().abbreviate(8).name();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
