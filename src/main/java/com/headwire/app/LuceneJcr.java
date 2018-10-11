/*
 * Li Qin
 */

package com.headwire.app;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
//import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
//import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import static org.junit.Assert.fail;

public class LuceneJcr {
    private final Logger log = LoggerFactory.getLogger(getClass());
    //private NodeStore nodeStore;
    private SegmentNodeStore nodeStore;
    private Repository repository;

    public void initNodeStore() throws IOException {
    	//File file = new File("target/"+System.currentTimeMillis());
        //FileStore fileStore = FileStore.Builder.create(file, 1);
    	//FileStore fileStore = FileStore.newFileStore(new File("target/"+System.currentTimeMillis())).create();
        //nodeStore = new SegmentNodeStore(fileStore);
    	FileStore fileStore;
    	try {
			fileStore = FileStoreBuilder.fileStoreBuilder(new File("Li-repository"+System.currentTimeMillis())).withBlobStore((BlobStore) new FileBlobStore("Li-repository/blob"+System.currentTimeMillis())).build();
			nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
		} catch (InvalidFileStoreVersionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	
    }

    public void initRepository() {
        LuceneIndexProvider provider = new LuceneIndexProvider();
        Jcr jcr = new Jcr(nodeStore)
                .withAsyncIndexing()
                .with(new LuceneIndexEditorProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .withAsyncIndexing();
        repository = jcr.createRepository();
        System.out.println("Repository initialized");
    }

    public void createLuceneIndex() throws RepositoryException {
        Session session = createAdminSession();
        Node lucene = JcrUtils.getOrCreateByPath("/oak:index/lucene", "oak:Unstructured",
                "oak:QueryIndexDefinition", session, false);
        lucene.setProperty("compatVersion", 2);
        lucene.setProperty("type", "lucene");
        lucene.setProperty("async", "async");
        Node rules = lucene.addNode("indexRules", "nt:unstructured");
        Node allProps = rules.addNode("nt:base")
                .addNode("properties", "nt:unstructured")
                .addNode("allProps", "oak:Unstructured");
        allProps.setProperty("name", ".*");
        allProps.setProperty("isRegexp", true);
        allProps.setProperty("nodeScopeIndex", true);
        session.save();
        session.logout();
        System.out.println("Lucene index created");
    }

    private void createTestData() throws RepositoryException {
        Session session = createAdminSession();

        //Node test = session.getRootNode().addNode("test");
        //test.setProperty("name", "torgeir");
        
        Node content = session.getRootNode().addNode("content");
        Node node1 = content.addNode("node1");
        Node test1 = node1.addNode("test");
        test1.setProperty("name", "torgeir1");
        String[] tags1 = {"tag1","tag2","tag3"};
        test1.setProperty("tags", tags1, 1);
        Node node2 = content.addNode("node2");
        Node test2 = node2.addNode("test");
        test2.setProperty("name", "torgeir2");
        String[] tags2 = {"tag4","tag5","tag6"};
        test2.setProperty("tags", tags2, 1);
        Node node3 = content.addNode("node3");
        Node test3 = node3.addNode("test");
        test3.setProperty("name", "torgeir");
        String[] tags3 = {"tag7","tag8","tag9"};
        test3.setProperty("tags", tags3, 1);
        
        session.save();
        session.logout();
        System.out.println("Testdata created");
    }

    private void performQuery() throws RepositoryException, InterruptedException {
        final Session session = createAdminSession();

        TimeUnit.SECONDS.sleep(5);
        System.out.println("Going to perform query");

        QueryManager qm  =session.getWorkspace().getQueryManager();
        // test
        String myQuery = "SELECT [jcr:path], [rep:facet(tags)] FROM [nt:base] AS s WHERE ISDESCENDANTNODE([/content/node1])";
        final Query q = qm.createQuery(myQuery, Query.JCR_SQL2);
        QueryResult result = q.execute();
        
        RowIterator rows = result.getRows();
        while(rows.hasNext()){
             Row row = rows.nextRow();
             System.out.println("row path: "+ row.getPath());
            
        }
        
        FacetResult facetResult = new FacetResult(result);
        Set<String> dimensions = facetResult.getDimensions(); // { "tags" }
        System.out.println("set size:" + dimensions.size());
        if(dimensions.size() > 0) {
	        List<FacetResult.Facet> facets = facetResult.getFacets("tags");
	        for (FacetResult.Facet facet : facets) {
	            String label = facet.getLabel();
	            int count = facet.getCount();
	            System.out.println("label: "+ label);
	        }
        }
        //final Query q = qm.createQuery("select * from [nt:base] where " + "contains(*,'torgeir')", Query.JCR_SQL2);
        /*
        new RetryLoop(new RetryLoop.Condition() {
            @Override
            public String getDescription() {
                return "Full text query";
            }

            @Override
            public boolean isTrue() throws Exception {
                QueryResult r = q.execute();
                return r.getNodes().hasNext();
            }
        }, 105, 200); */

        System.out.println(q.execute().getNodes().next());
    }

    private Session createAdminSession() throws RepositoryException {
        return repository.login(getAdminCredentials());
    }

    private SimpleCredentials getAdminCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

    public static void main(String[] args) throws Exception {
    	System.out.println("in main now:");
        LuceneJcr test = new LuceneJcr();
        test.setUp();
    }

    private void setUp() throws Exception {
        initNodeStore();
        initRepository();
        createLuceneIndex();
        createTestData();
        performQuery();
    }


    private static class RetryLoop {
        private final long timeout;

        static public interface Condition {
            /**
             * Used in failure messages to describe what was expected
             */
            String getDescription();

            /**
             * If true we stop retrying. The RetryLoop retries on AssertionError,
             * so if tests fail in this method they are not reported as
             * failures but retried.
             */
            boolean isTrue() throws Exception;
        }

        public RetryLoop(Condition c, int timeoutSeconds, int intervalBetweenTriesMsec) {
            timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;
            while (System.currentTimeMillis() < timeout) {
                try {
                    if (c.isTrue()) {
                        return;
                    }
                } catch (AssertionError ae) {
                    // Retry JUnit tests failing in the condition as well
                    reportException(ae);
                } catch (Exception e) {
                    reportException(e);
                }

                try {
                    Thread.sleep(intervalBetweenTriesMsec);
                } catch (InterruptedException ignore) {
                }
            }

            onTimeout();
            /*fail("RetryLoop failed, condition is false after " + timeoutSeconds + " seconds: "
                    + c.getDescription()); */
        }

        /**
         * Can be overridden to report Exceptions that happen in the retry loop
         */
        protected void reportException(Throwable t) {
        }

        /**
         * Called if the loop times out without success, just before failing
         */
        protected void onTimeout() {
        }
    }


}