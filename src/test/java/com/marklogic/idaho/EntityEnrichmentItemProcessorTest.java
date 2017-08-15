package com.marklogic.idaho;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentPatchBuilder;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.helper.DatabaseClientProvider;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.DocumentPatchHandle;
import com.marklogic.client.query.CountedDistinctValue;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.junit.spring.AbstractSpringTest;
import com.marklogic.spring.batch.config.MarkLogicBatchConfiguration;
import com.marklogic.spring.batch.item.reader.ValuesItemReader;
import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import com.marklogic.xcc.template.XccTemplate;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.Map;

@ContextConfiguration(classes = { MarkLogicBatchConfiguration.class } )
public class EntityEnrichmentItemProcessorTest extends AbstractSpringTest {

    XMLDocumentManager docMgr;
    private final String COLLECTION_NAME = "source";
    private ApplicationContext applicationContext;

    DatabaseClientProvider databaseClientProvider;

    @Autowired
    public void setDatabaseClientProvider(DatabaseClientProvider databaseClientProvider) {
        this.databaseClientProvider = databaseClientProvider;
    }

    @Before
    public void setup() {
        DatabaseClient client = databaseClientProvider.getDatabaseClient();
        docMgr = client.newXMLDocumentManager();
        StringHandle text1 = new StringHandle("<doc><text>Abbey D'Agostino finished the race Tuesday after helping Nikki Hamblin of New Zealand back up and urging her to finish. The two clipped heels during the late part of the race and tumbled to the ground. Hamblin has indicated she will run in the final.  Emma Coburn, who took bronze in the women's 3,000 steeplechase, becoming the first American woman to medal in the event, reacted Wednesday</text></doc>");
        DocumentMetadataHandle handle = new DocumentMetadataHandle();
        handle.withCollections(COLLECTION_NAME);
        docMgr.write("hello.xml", handle, text1);
    }
    
    @Test
    public void testNamedEntityEnrichmentTest() throws Exception {
        assertTrue(docMgr.read("hello.xml").hasContent());
        EntityEnrichmentItemProcessor processor =
            new EntityEnrichmentItemProcessor(
                    databaseClientProvider.getDatabaseClient(),
                    "src/main/resources/nlp/tokenizer/en-token.bin",
                    "src/main/resources/nlp/namefinder/en-ner-person.bin");

        QueryDefinition qd = new StructuredQueryBuilder().collection(COLLECTION_NAME);
        ValuesItemReader reader = new ValuesItemReader(databaseClientProvider.getDatabaseClient(), getQueryOptions(), "uris", qd);
        reader.open(null);
        CountedDistinctValue val = reader.read();
        String[] info = processor.process(val);
        assertTrue(info[0].equals("hello.xml"));
        logger.info(info[0]);
        logger.info(info[1]);
        DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = db.parse(
                new InputSource(
                        new StringReader(info[1])
                )
        );
        
        NodeList list = doc.getElementsByTagName("name");
        assertTrue(list.item(0).getTextContent().equals("Hamblin"));
        assertTrue(list.item(1).getTextContent().equals("Emma Coburn"));

        XMLDocumentManager docMgr = databaseClientProvider.getDatabaseClient().newXMLDocumentManager();
        
        DocumentPatchBuilder xmlPatchBldr = docMgr.newPatchBuilder();
        DocumentPatchHandle patchHandle = xmlPatchBldr.insertFragment("/doc", DocumentPatchBuilder.Position.LAST_CHILD, "<test />").build();
        docMgr.patch("hello.xml", patchHandle);
        
    }

    public StringHandle getQueryOptions() {
        return new StringHandle("<options xmlns=\"http://marklogic.com/appservices/search\">\n" +
                "    <search-option>unfiltered</search-option>\n" +
                "    <quality-weight>0</quality-weight>\n" +
                "    <values name=\"uris\">\n" +
                "        <uri/>\n" +
                "    </values>\n" +
                "</options>");
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        setDatabaseClientProvider(applicationContext.getBean("databaseClientProvider", DatabaseClientProvider.class));
        Map<String, XccTemplate> map = applicationContext.getBeansOfType(XccTemplate.class);
        if (map.size() == 1) {
            setXccTemplate(map.values().iterator().next());
        }
    }
}
