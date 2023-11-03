package ai.langstream.cli.commands.doc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ObjectUtils;
import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(name = "generate-documentation",
        mixinStandardHelpOptions = true,
        header = "Generate CLI JSON documentation",
        helpCommand = true
)
public class GenerateDocumentation implements Runnable {

    ObjectMapper mapper = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @SneakyThrows
    public void run() {
        final CommandLine.Model.CommandSpec spec = this.spec.root();
        final CommandDoc result = gen(spec);
        System.out.println(mapper.writeValueAsString(result));
    }

    private static CommandDoc gen(CommandLine.Model.CommandSpec spec) {
        final CommandDoc commandDoc = new CommandDoc();
        commandDoc.setName(spec.name());
        commandDoc.setDescription(getDescription(spec));
        List<CommandArgDoc> argsDocs = new ArrayList<>();
        System.out.println(spec.options());
        for (CommandLine.Model.ArgSpec arg : spec.args()) {
            if (!arg.isPositional()) {
                continue;
            }
            final CommandArgDoc commandArgDoc = new CommandArgDoc();
            commandArgDoc.setName(arg.paramLabel());
            commandArgDoc.setDescription(arg.description()[0]);
            commandArgDoc.setDefaultValue(arg.defaultValue());
            commandArgDoc.setRequired(arg.required());
            commandArgDoc.setType(arg.type().getSimpleName());
            argsDocs.add(commandArgDoc);
        }
        commandDoc.setArgs(argsDocs);

        List<CommandOptionDoc> optionsDoc = new ArrayList<>();
        System.out.println(spec.options());
        for (CommandLine.Model.OptionSpec option : spec.options()) {
            if (option.isPositional()) {
                continue;
            }
            final CommandOptionDoc optionDoc = new CommandOptionDoc();
            optionDoc.setNames(Arrays.asList(option.names()));
            optionDoc.setDescription(option.description()[0]);
            optionDoc.setDefaultValue(option.defaultValue());
            optionDoc.setRequired(option.required());
            optionDoc.setType(option.type().getSimpleName());
            optionsDoc.add(optionDoc);
        }
        commandDoc.setOptions(optionsDoc);
        List<CommandDoc> subcommands = new ArrayList<>();
        for (CommandLine value : spec.subcommands().values()) {
           subcommands.add(gen(value.getCommandSpec()));
        }
        commandDoc.setSubcommands(subcommands);
        return commandDoc;
    }

    private static String getDescription(CommandLine.Model.CommandSpec spec) {
        return ObjectUtils.getFirstNonNull(
                () -> spec.usageMessage().headerHeading(),
                () -> spec.usageMessage().descriptionHeading(),
                () -> spec.usageMessage().description().length == 0 ? null : spec.usageMessage().description()[0]
        );
    }


    @Getter
    @Setter
    @Data
    @NoArgsConstructor
    public static class CommandDoc {
        private String name;
        private String description;
        private List<CommandArgDoc> args;
        private List<CommandOptionDoc> options;
        private List<CommandDoc> subcommands;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CommandArgDoc {
        String name;
        private String description;
        boolean required;
        private String type;
        private Object defaultValue;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CommandOptionDoc {
        List<String> names;
        private String description;
        boolean required;
        private String type;
        private Object defaultValue;
    }

}
