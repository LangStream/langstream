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
package ai.langstream.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@WireMockTest
class ComputeEmbeddingsIT extends AbstractApplicationRunner {

    @AllArgsConstructor
    private static class EmbeddingsConfig {
        String model;
        String modelUrl;
        String providerConfiguration;
        Runnable stubMakers;
        Set<String> expectedEmbeddings;

        @Override
        public String toString() {
            return "EmbeddingsConfig{" + "model='" + model + '\'' + '}';
        }
    }

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    private static Stream<Arguments> providers() {
        Arguments vertex =
                Arguments.of(
                        new EmbeddingsConfig(
                                "textembedding-gecko",
                                null,
                                """
                                     configuration:
                                         resources:
                                            - type: "vertex-configuration"
                                              name: "Vertex configuration"
                                              configuration:
                                                url: "%s"
                                                region: "us-east1"
                                                project: "the-project"
                                                token: "some-token"
                                """
                                        .formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                                () ->
                                        stubFor(
                                                post("/v1/projects/the-project/locations/us-east1/publishers/google/models/textembedding-gecko:predict")
                                                        .willReturn(
                                                                okJson(
                                                                        """
                                                                                                   {
                                                                                                      "predictions": [
                                                                                                        {
                                                                                                          "embeddings": {
                                                                                                            "statistics": {
                                                                                                              "truncated": false,
                                                                                                              "token_count": 6
                                                                                                            },
                                                                                                            "values": [ 1.0, 5.4, 8.7]
                                                                                                          }
                                                                                                        }
                                                                                                      ]
                                                                                                    }
                                                                        """))),
                                Set.of("[1.0,5.4,8.7]")));
        Arguments openAi =
                Arguments.of(
                        new EmbeddingsConfig(
                                "text-embedding-ada-002",
                                null,
                                """
                                 configuration:
                                      resources:
                                        - type: "open-ai-configuration"
                                          name: "OpenAI Azure configuration"
                                          configuration:
                                            url: "%s"
                                            access-key: "xxx"
                                            provider: "azure"
                                """
                                        .formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                                () ->
                                        stubFor(
                                                post("/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-08-01-preview")
                                                        .willReturn(
                                                                okJson(
                                                                        """
                                                                                                       {
                                                                                                           "data": [
                                                                                                             {
                                                                                                               "embedding": [1.0, 5.4, 8.7],
                                                                                                               "index": 0,
                                                                                                               "object": "embedding"
                                                                                                             }
                                                                                                           ],
                                                                                                           "model": "text-embedding-ada-002",
                                                                                                           "object": "list",
                                                                                                           "usage": {
                                                                                                             "prompt_tokens": 5,
                                                                                                             "total_tokens": 5
                                                                                                           }
                                                                                                         }
                                                                        """))),
                                Set.of("[1.0,5.4,8.7]")));
        Arguments huggingFaceApi =
                Arguments.of(
                        new EmbeddingsConfig(
                                "some-model",
                                null,
                                """
                                     configuration:
                                         resources:
                                            - type: "hugging-face-configuration"
                                              name: "Hugging Face API configuration"
                                              configuration:
                                                api-url: "%s"
                                                model-check-url: "%s"
                                                access-key: "some-token"
                                                provider: "api"
                                """
                                        .formatted(
                                                wireMockRuntimeInfo.getHttpBaseUrl()
                                                        + "/embeddings/",
                                                wireMockRuntimeInfo.getHttpBaseUrl()
                                                        + "/modelcheck/"),
                                () -> {
                                    stubFor(
                                            get("/modelcheck/some-model")
                                                    .willReturn(
                                                            okJson(
                                                                    "{\"modelId\": \"some-model\",\"tags\": [\"sentence-transformers\"]}")));
                                    stubFor(
                                            post("/embeddings/some-model")
                                                    .willReturn(okJson("[[1.0, 5.4, 8.7]]")));
                                },
                                Set.of("[1.0,5.4,8.7]")));
        Arguments hugginFaceLocal =
                Arguments.of(
                        new EmbeddingsConfig(
                                "multilingual-e5-small",
                                "djl://ai.djl.huggingface.pytorch/intfloat/multilingual-e5-small",
                                """
                                     configuration:
                                         resources:
                                            - type: "hugging-face-configuration"
                                              name: "Hugging Face API configuration"
                                              configuration:
                                                provider: "local"
                                """,
                                () -> {},
                                Set.of(
                                        // on Mac
                                        "[0.0578145831823349,-0.02821592427790165,-0.06973546743392944,-0.11966443806886673,0.09719318151473999,-0.02437288500368595,0.037173960357904434,0.06334057450294495,0.0038273059763014317,0.016641629859805107,0.034420136362314224,0.03339029476046562,0.04662078619003296,-0.05664201080799103,-0.020073235034942627,-0.001992315286770463,0.06202026456594467,-0.013051013462245464,-0.022022254765033722,-0.04521115496754646,0.023956529796123505,-0.01018081046640873,-0.024736007675528526,0.041859593242406845,0.03387194871902466,0.0440528467297554,-0.03393081948161125,-0.00129299599211663,0.05213664844632149,-0.057084821164608,-0.04214051365852356,-0.035878147929906845,0.054346468299627304,-0.0429738312959671,0.02415909431874752,0.06621567159891129,-0.019626449793577194,-0.054620448499917984,0.03975465148687363,-0.025969630107283592,-0.03388348966836929,0.011065256781876087,0.06274556368589401,0.04584011062979698,0.039662301540374756,0.03268987685441971,-0.04992841184139252,0.014321385882794857,-0.05076282471418381,-0.040383629500865936,-0.03865436837077141,0.061364199966192245,-0.0014176814584061503,0.03975527733564377,0.02880760468542576,-0.06463174521923065,-0.056210312992334366,-0.10111309587955475,-0.04404542222619057,0.0804879367351532,0.11694527417421341,0.001967468298971653,-0.033113379031419754,0.023246848955750465,0.10981788486242294,0.04703543707728386,0.04124673455953598,0.03862600401043892,-0.03987414762377739,0.006217349786311388,-0.04964762181043625,0.03969116136431694,-0.015994593501091003,-0.021580221131443977,0.05632168427109718,0.014246135950088501,0.012250794097781181,-0.0432860292494297,0.06466314196586609,-0.014426779933273792,-0.08301924169063568,-0.04861508309841156,-0.04693108797073364,0.06518333405256271,-0.0710906833410263,0.0877121314406395,0.050791967660188675,-0.07740598171949387,0.07244061678647995,0.010154551826417446,0.01482943631708622,0.06174071878194809,-0.0740501657128334,-0.05000511556863785,-0.12823283672332764,-0.0518074706196785,-0.03637321665883064,0.05512413755059242,0.05685008317232132,-0.051654696464538574,0.05225558206439018,-0.04414035379886627,0.049583178013563156,-0.04327954351902008,-0.04093344137072563,0.036944430321455,-0.006889823824167252,-0.0332576185464859,0.03596322238445282,-0.0278958547860384,-0.01232653297483921,0.009900200180709362,0.04956604912877083,0.04884594678878784,-0.07964546233415604,-0.052461057901382446,-0.03006182610988617,-0.04834115505218506,0.032082896679639816,-0.04966232180595398,0.07178604602813721,-0.03326759114861488,-0.039971500635147095,-0.03450896218419075,-0.03569018840789795,-0.04421060159802437,0.007395340595394373,0.0382993221282959,0.04020233079791069,0.02893606573343277,0.10297822952270508,0.07202310115098953,0.03995693475008011,0.004884864203631878,0.021106665953993797,0.0674026757478714,-0.028800878673791885,-0.03921280428767204,-0.015176347456872463,-0.057897791266441345,-0.05764665827155113,0.05894194170832634,-0.08333422243595123,0.0166957825422287,0.07699475437402725,0.09090491384267807,0.08291497826576233,0.0058735753409564495,0.03520221263170242,-0.030117498710751534,0.02248154580593109,-0.037330251187086105,0.03592915087938309,0.02937120571732521,0.03691040351986885,-0.05061875283718109,-0.06473111361265182,-0.06461644917726517,0.06092718243598938,0.06360158324241638,-0.0332927331328392,-0.06697344779968262,-0.0665106475353241,-0.0038964804261922836,-0.06770992279052734,-0.044745709747076035,0.02278999611735344,0.0710294246673584,-0.034262970089912415,-0.03189576789736748,-0.08274378627538681,0.007720119785517454,-0.035202912986278534,0.015703298151493073,-0.008380340412259102,0.04853912442922592,-0.050737008452415466,0.08057953417301178,0.06560815125703812,0.026573320850729942,-0.0201464481651783,0.0076991114765405655,-0.0938519760966301,-0.04616386070847511,-0.04844296723604202,-0.02564549446105957,-0.07073294371366501,0.016774946823716164,0.05364406481385231,-0.03489692881703377,-0.02407420426607132,0.04655499383807182,-0.010772611945867538,-0.06840717792510986,-0.04433761537075043,0.026063092052936554,-0.020840846002101898,0.05549715831875801,0.06160842254757881,0.02022039331495762,-0.04580838605761528,-0.05382368713617325,0.007199299056082964,0.03451785817742348,0.03275618702173233,0.009159817360341549,-0.032958947122097015,0.0812787115573883,-0.04438749700784683,0.028742380440235138,0.04900388792157173,-0.07307273894548416,-0.10819397121667862,0.023443903774023056,-0.08766500651836395,-0.018628614023327827,0.01944996416568756,0.08913910388946533,-0.019191015511751175,5.079195252619684E-4,0.08846462517976761,-0.02754773572087288,0.07973919063806534,-0.04745294153690338,-0.05402524769306183,0.013893207535147667,0.12736062705516815,-0.09502208977937698,-0.02762526646256447,0.031247470527887344,-0.05753298103809357,-0.030252661556005478,-0.059533726423978806,-0.06387960910797119,-0.041012708097696304,-0.055293962359428406,-0.03474295139312744,-0.014155738055706024,0.1026943027973175,-0.06564648449420929,0.012192616239190102,-0.03828966245055199,0.045491039752960205,-0.06906663626432419,0.05991367995738983,-0.03157135099172592,-0.03447442501783371,0.04739203304052353,0.0035037570632994175,0.04793522506952286,7.109076250344515E-4,-0.06075091287493706,-0.06969919055700302,-0.04045889526605606,-0.03888397663831711,0.07095940411090851,0.05736815929412842,0.04222995042800903,-0.08837233483791351,0.0473332516849041,0.029656926169991493,0.016456281766295433,0.11562099307775497,0.04985729604959488,0.024185622110962868,0.07614585757255554,-0.03714750334620476,-0.0034233832266181707,-0.0905488133430481,-0.024084547534585,-0.0546550527215004,0.010959787294268608,0.03652103990316391,-0.036811087280511856,0.001075870473869145,-0.07994946837425232,0.050391603261232376,0.0996408760547638,-0.07707351446151733,-0.051416266709566116,0.07016818225383759,0.055183134973049164,0.04416630044579506,0.07491583377122879,0.06337472796440125,-0.0194217748939991,0.025521302595734596,0.047719452530145645,-0.008756628260016441,-0.03449970483779907,-0.02132328972220421,-0.028398672118782997,0.08536966145038605,-0.06472010165452957,0.05955315753817558,0.023550478741526604,-0.01980086788535118,0.03646115958690643,-0.02347954548895359,0.06112123280763626,-0.022198159247636795,-0.05335048586130142,0.04470120742917061,0.02880634367465973,-0.016072802245616913,0.058053821325302124,0.01114592980593443,0.0330808088183403,0.03197785094380379,0.04821407422423363,0.07429102808237076,0.029402414336800575,-0.055901624262332916,-0.08954484760761261,0.03995891660451889,0.07816694676876068,-0.024604661390185356,0.0620579831302166,-0.058921005576848984,-0.045178063213825226,-0.035820815712213516,-0.046394918113946915,-0.01388606894761324,-0.0029846476390957832,0.01987309753894806,-0.01942511461675167,-0.02969549223780632,-0.051134027540683746,0.03009149618446827,-0.028684666380286217,0.05199574679136276,-0.07542966306209564,-0.07191123813390732,0.06451542675495148,-0.050691962242126465,-0.028370806947350502,-0.005663217976689339,0.02270820550620556,-0.03222748637199402,-0.04935092106461525,0.03303603082895279,0.061597906053066254,-0.06660722196102142,0.06992559134960175,-0.05570777505636215,-0.056695643812417984,0.06262296438217163,-0.04167942330241203,-0.03416252136230469,0.0708986297249794,0.041380465030670166,-0.10464166849851608,-0.013665161095559597,0.029044080525636673,-0.03487810119986534,0.01304139569401741,-0.10283218324184418,-0.03500160574913025,0.03310192748904228,0.05125002562999725,-0.03000393509864807,-0.061212655156850815,0.006450562737882137,0.015301967039704323,0.029728908091783524,0.02017238549888134,-0.029545778408646584,-0.004302792716771364,0.03971683233976364,-0.05546005070209503,0.09130572527647018,0.03391131013631821,-0.03954692184925079,0.006116618402302265,-0.043929412961006165,-0.07805261015892029,-0.07272732257843018,0.061641812324523926,-0.07524901628494263,-0.007530458737164736,0.04592833295464516,0.042143676429986954,0.007589149288833141,0.046104829758405685]",
                                        // on Linux (GH Actions) all the values have a different
                                        // precision
                                        "[0.05781453102827072,-0.02821594476699829,-0.06973543763160706,-0.11966442316770554,0.09719318896532059,-0.024372870102524757,0.03717394545674324,0.06334058940410614,0.003827278967946768,0.016641611233353615,0.03442015126347542,0.03339030593633652,0.046620775014162064,-0.056642018258571625,-0.020073307678103447,-0.001992354402318597,0.06202023848891258,-0.01305102463811636,-0.02202223800122738,-0.04521108791232109,0.023956524208188057,-0.010180830024182796,-0.024735989049077034,0.041859693825244904,0.03387196734547615,0.044052865356206894,-0.03393082693219185,-0.0012930165976285934,0.05213667079806328,-0.05708485469222069,-0.04214050620794296,-0.03587818518280983,0.05434645712375641,-0.04297379031777382,0.024159112945199013,0.06621565669775009,-0.019626405090093613,-0.05462045595049858,0.03975464031100273,-0.02596956491470337,-0.03388350456953049,0.011065248399972916,0.06274563074111938,0.04584014043211937,0.03966227546334267,0.032689858227968216,-0.049928463995456696,0.014321382157504559,-0.05076286569237709,-0.040383610874414444,-0.0386543832719326,0.06136414781212807,-0.001417670864611864,0.039755262434482574,0.028807541355490685,-0.0646316409111023,-0.056210316717624664,-0.10111312568187714,-0.044045381247997284,0.08048786222934723,0.11694535613059998,0.001967519987374544,-0.03311336413025856,0.02324688620865345,0.10981791466474533,0.047035448253154755,0.04124680534005165,0.03862597420811653,-0.03987416997551918,0.0062173400074243546,-0.04964764788746834,0.039691176265478134,-0.015994561836123466,-0.02158023603260517,0.05632172152400017,0.014246108941733837,0.012250805273652077,-0.04328606277704239,0.0646631270647049,-0.014426754787564278,-0.08301929384469986,-0.048615097999572754,-0.04693107679486275,0.06518331915140152,-0.07109065353870392,0.08771209418773651,0.05079199746251106,-0.07740595191717148,0.07244058698415756,0.01015458907932043,0.014829417690634727,0.061740677803754807,-0.0740501880645752,-0.05000508204102516,-0.12823285162448883,-0.05180748924612999,-0.03637325018644333,0.055124133825302124,0.056850068271160126,-0.05165468156337738,0.0522555410861969,-0.04414036497473717,0.04958313703536987,-0.04327947646379471,-0.04093344137072563,0.03694439306855202,-0.006889817770570517,-0.03325757756829262,0.035963255912065506,-0.027895795181393623,-0.012326515279710293,0.009900184348225594,0.049566030502319336,0.048845890909433365,-0.07964548468589783,-0.052461057901382446,-0.030061783269047737,-0.048341210931539536,0.03208288550376892,-0.04966229945421219,0.07178604602813721,-0.03326769545674324,-0.0399714857339859,-0.034508951008319855,-0.035690151154994965,-0.04421064257621765,0.007395290303975344,0.03829936310648918,0.04020233452320099,0.0289361160248518,0.10297815501689911,0.07202307134866714,0.03995692357420921,0.004884844645857811,0.021106677129864693,0.06740269064903259,-0.0288008414208889,-0.039212845265865326,-0.01517635490745306,-0.05789779871702194,-0.057646650820970535,0.05894193425774574,-0.08333422243595123,0.016695799306035042,0.07699471712112427,0.09090491384267807,0.08291497826576233,0.005873580928891897,0.03520219027996063,-0.030117470771074295,0.022481486201286316,-0.03733021393418312,0.035929128527641296,0.029371270909905434,0.03691040724515915,-0.050618793815374374,-0.06473106890916824,-0.06461647152900696,0.06092720106244087,0.0636015236377716,-0.03329279273748398,-0.066973477602005,-0.06651061773300171,-0.0038965167477726936,-0.06770990788936615,-0.04474564269185066,0.02278999797999859,0.0710294097661972,-0.034262921661138535,-0.03189576789736748,-0.08274372667074203,0.007720154244452715,-0.03520296886563301,0.015703318640589714,-0.00838035810738802,0.04853915050625801,-0.05073702335357666,0.08057960122823715,0.06560812145471573,0.02657333016395569,-0.02014649473130703,0.007699077483266592,-0.09385202080011368,-0.04616383835673332,-0.04844289273023605,-0.025645479559898376,-0.07073293626308441,0.01677490398287773,0.0536440908908844,-0.034896980971097946,-0.024074260145425797,0.04655497893691063,-0.010772603563964367,-0.06840716302394867,-0.04433763027191162,0.0260631013661623,-0.02084086649119854,0.05549710988998413,0.06160847470164299,0.02022041380405426,-0.04580836370587349,-0.053823597729206085,0.007199328392744064,0.03451785817742348,0.032756246626377106,0.009159853681921959,-0.03295895457267761,0.0812787115573883,-0.044387493282556534,0.028742363676428795,0.04900383576750755,-0.07307272404432297,-0.10819391906261444,0.02344393916428089,-0.08766502141952515,-0.018628640100359917,0.019449936226010323,0.08913910388946533,-0.019191045314073563,5.079953698441386E-4,0.08846462517976761,-0.027547668665647507,0.07973915338516235,-0.04745294526219368,-0.054025229066610336,0.013893170282244682,0.12736059725284576,-0.09502208232879639,-0.02762528508901596,0.031247474253177643,-0.05753292888402939,-0.030252689495682716,-0.05953374132514,-0.06387961655855179,-0.04101262986660004,-0.0552939772605896,-0.03474295139312744,-0.014155722223222256,0.10269433259963989,-0.0656464695930481,0.012192646972835064,-0.03828964754939079,0.045491065829992294,-0.06906665861606598,0.059913672506809235,-0.031571321189403534,-0.03447446972131729,0.04739197716116905,0.003503733780235052,0.047935232520103455,7.108924328349531E-4,-0.06075088679790497,-0.06969919055700302,-0.040458906441926956,-0.03888401761651039,0.07095938920974731,0.05736818537116051,0.04222990572452545,-0.08837225288152695,0.047333210706710815,0.02965693362057209,0.01645626500248909,0.11562100797891617,0.04985729977488518,0.024185605347156525,0.07614587992429733,-0.03714749589562416,-0.0034232879988849163,-0.09054877609014511,-0.024084605276584625,-0.05465508624911308,0.010959780775010586,0.03652108833193779,-0.03681115061044693,0.0010759062133729458,-0.07994949817657471,0.05039156973361969,0.0996408760547638,-0.07707351446151733,-0.05141624063253403,0.07016823440790176,0.05518321692943573,0.044166263192892075,0.07491588592529297,0.06337472051382065,-0.01942180097103119,0.025521324947476387,0.047719430178403854,-0.008756620809435844,-0.034499697387218475,-0.021323315799236298,-0.02839864231646061,0.08536964654922485,-0.06472007930278778,0.05955316498875618,0.023550493642687798,-0.019800856709480286,0.03646118938922882,-0.023479580879211426,0.06112123280763626,-0.02219819277524948,-0.053350500762462616,0.04470121115446091,0.028806408867239952,-0.016072792932391167,0.05805380642414093,0.011145914904773235,0.033080797642469406,0.031977806240320206,0.04821406304836273,0.07429107278585434,0.02940245531499386,-0.055901654064655304,-0.08954483270645142,0.03995896875858307,0.0781669020652771,-0.024604711681604385,0.06205795705318451,-0.05892108753323555,-0.04517805576324463,-0.035820845514535904,-0.04639491066336632,-0.013886061497032642,-0.0029846476390957832,0.01987306959927082,-0.019425079226493835,-0.02969541773200035,-0.05113404989242554,0.03009149618446827,-0.02868463099002838,0.05199575051665306,-0.07542966306209564,-0.0719112679362297,0.06451541930437088,-0.05069193243980408,-0.028370792046189308,-0.0056632631458342075,0.022708265110850334,-0.032227471470832825,-0.04935089871287346,0.033036008477211,0.061597902327775955,-0.06660719215869904,0.06992559134960175,-0.05570785328745842,-0.056695617735385895,0.06262297183275223,-0.04167940840125084,-0.03416254371404648,0.07089859992265701,0.04138045012950897,-0.10464167594909668,-0.013665147125720978,0.02904410846531391,-0.034878116101026535,0.013041423633694649,-0.10283213108778,-0.035001643002033234,0.03310190513730049,0.05124998465180397,-0.030003918334841728,-0.06121263653039932,0.006450550630688667,0.015301971696317196,0.029728908091783524,0.020172378048300743,-0.029545795172452927,-0.004302808083593845,0.039716873317956924,-0.05546009913086891,0.09130572527647018,0.033911388367414474,-0.03954692184925079,0.006116645410656929,-0.04392937198281288,-0.07805268466472626,-0.0727272778749466,0.061641816049814224,-0.07524903118610382,-0.007530380506068468,0.04592826962471008,0.042143695056438446,0.007589129265397787,0.0461047887802124]")));
        return Stream.of(vertex, openAi, huggingFaceApi, hugginFaceLocal);
    }

    @ParameterizedTest
    @MethodSource("providers")
    public void testComputeEmbeddings(EmbeddingsConfig config) throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        config.stubMakers.run();
        // wait for WireMock to be ready
        Thread.sleep(1000);

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        config.providerConfiguration,
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      model-url: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "something to embed"
                                      concurrency: 1
                                      flush-interval: 0
                                """
                                .formatted(
                                        inputTopic,
                                        outputTopic,
                                        inputTopic,
                                        outputTopic,
                                        config.model,
                                        config.modelUrl));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName(inputTopic)))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                // produce one message to the input-topic
                sendMessage(
                        inputTopic,
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                new Consumer<String>() {
                                    @Override
                                    public void accept(String msg) {
                                        Set<String> expectedMessages = new HashSet<>();
                                        for (String embedding : config.expectedEmbeddings) {
                                            String expected =
                                                    "{\"name\":\"some name\",\"description\":\"some description\",\"embeddings\":%s}"
                                                            .formatted(embedding);
                                            expectedMessages.add(expected);
                                        }
                                        if (!expectedMessages.contains(msg)) {
                                            fail(
                                                    "Unexpected message "
                                                            + msg
                                                            + ", it should be one of "
                                                            + expectedMessages);
                                        }
                                    }
                                }));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testComputeBatchEmbeddings(boolean sameKey) throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        String embeddingFirst = "[1.0,5.4,8.7]";
        String embeddingSecond = "[2.0,5.4,8.7]";
        String embeddingThird = "[3.0,5.4,8.7]";
        stubFor(
                post("/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-08-01-preview")
                        .willReturn(
                                okJson(
                                        """
                                               {
                                                   "data": [
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     },
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     },
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     }
                                                   ],
                                                   "model": "text-embedding-ada-002",
                                                   "object": "list",
                                                   "usage": {
                                                     "prompt_tokens": 5,
                                                     "total_tokens": 5
                                                   }
                                                 }
                                            """
                                                .formatted(
                                                        embeddingFirst,
                                                        embeddingSecond,
                                                        embeddingThird))));
        // wait for WireMock to be ready
        Thread.sleep(1000);

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};
        String model = "text-embedding-ada-002";

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                               configuration:
                                 resources:
                                   - type: "open-ai-configuration"
                                     name: "OpenAI Azure configuration"
                                     configuration:
                                       url: "%s"
                                       access-key: "%s"
                                       provider: "azure"
                               """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl(), "sdòflkjsòlfkj"),
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 100
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "something to embed"
                                      batch-size: 3
                                      concurrency: 4
                                      flush-interval: 10000
                                """
                                .formatted(
                                        inputTopic, outputTopic, inputTopic, outputTopic, model));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName(inputTopic)))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                // produce 10 messages to the input-topic
                List<String> expected = new ArrayList<>();
                for (int i = 0; i < 9; i++) {
                    String name = "name_" + i;
                    String key = sameKey ? "key" : "key_" + (i % 3);
                    sendMessage(
                            inputTopic,
                            key,
                            "{\"name\": \" " + name + "\", \"description\": \"some description\"}",
                            List.of(),
                            producer);

                    String embeddings;
                    if (sameKey) {
                        if (i % 3 == 0) {
                            embeddings = embeddingFirst;
                        } else if (i % 3 == 1) {
                            embeddings = embeddingSecond;
                        } else {
                            embeddings = embeddingThird;
                        }
                    } else {
                        // this may look weird, but given the key distribution, we build 3 batches
                        // that contain 3 messages each
                        // the first 3 messages become the head of each batch, the next 3 messages
                        // are the second element of each batch, and so on
                        embeddings =
                                switch (i) {
                                    case 0, 1, 2 -> embeddingFirst;
                                    case 3, 4, 5 -> embeddingSecond;
                                    case 6, 7, 8 -> embeddingThird;
                                    default -> throw new IllegalStateException();
                                };
                    }
                    String expectedContent =
                            "{\"name\":\" "
                                    + name
                                    + "\",\"description\":\"some description\",\"embeddings\":"
                                    + embeddings
                                    + "}";
                    expected.add(expectedContent);
                }

                executeAgentRunners(applicationRuntime);

                if (sameKey) {
                    // all the messages have the same key, so they must be processed in order
                    waitForMessages(consumer, expected);
                } else {
                    waitForMessagesInAnyOrder(consumer, expected);
                }
            }
        }
    }

    @Test
    public void testLegacySyntax() throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        String embeddingFirst = "[1.0,5.4,8.7]";
        stubFor(
                post("/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-08-01-preview")
                        .withRequestBody(equalTo("{\"input\":[\"something to embed foo\"]}"))
                        .willReturn(
                                okJson(
                                        """
                                               {
                                                   "data": [
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     }
                                                   ],
                                                   "model": "text-embedding-ada-002",
                                                   "object": "list",
                                                   "usage": {
                                                     "prompt_tokens": 5,
                                                     "total_tokens": 5
                                                   }
                                                 }
                                            """
                                                .formatted(embeddingFirst))));
        // wait for WireMock to be ready
        Thread.sleep(1000);

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};
        String model = "text-embedding-ada-002";

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                               configuration:
                                 resources:
                                   - type: "open-ai-configuration"
                                     name: "OpenAI Azure configuration"
                                     configuration:
                                       url: "%s"
                                       access-key: "%s"
                                       provider: "azure"
                               """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl(), "sdòflkjsòlfkj"),
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 100
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "%s"
                                      batch-size: 3
                                      concurrency: 4
                                      flush-interval: 10000
                                """
                                .formatted(
                                        inputTopic,
                                        outputTopic,
                                        inputTopic,
                                        outputTopic,
                                        model,
                                        "something to embed {{% value.name}}"));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName(inputTopic)))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                sendMessage(inputTopic, null, "{\"name\": \"foo\"}", List.of(), producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer, List.of("{\"name\":\"foo\",\"embeddings\":[1.0,5.4,8.7]}"));
            }
        }
    }
}
