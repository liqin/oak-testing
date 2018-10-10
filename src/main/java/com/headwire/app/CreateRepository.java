package com.headwire.app;

import java.util.List;
import java.util.Set;

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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.query.facet.FacetResult;


/**
 * Created by lq on 10/10/2018.
 */
public class CreateRepository {

    public static void main (String[] args) {
        
    	// Test adding a node
    	addNode();
    	
    	// Test indexing some node
    	indexNode();
    	
    	// Test facet search node

    }
    
    private static void indexNode() {
    	
    	// Create repository
    	Repository repo = new Jcr(new Oak()).createRepository();
    	
    	// Login to repository
    	Session session = null;
    	
    	try {
			session = repo.login(
			        new SimpleCredentials("admin", "admin".toCharArray()));
			session.save();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
    	
    	// Todo Indexing some nodes
    	try {
			Node root = session.getRootNode();
			if (!root.hasNode("hello1")) {
				System.out.println("creating the hello1 node");
				root.addNode("hello1");
				if (root.hasNode("hello1")) {
					System.out.println("hello1 node created successfully");
				}
			}
			if (!root.hasNode("hello2")) {
				System.out.println("creating the hello2 node");
				root.addNode("hello2");
				if (root.hasNode("hello2")) {
					System.out.println("hello2 node created successfully");
				}
			}
			if (!root.hasNode("hello3")) {
				System.out.println("creating the hello3 node");
				root.addNode("hello3");
				if (root.hasNode("hello3")) {
					System.out.println("hello3 node created successfully");
				}
			}
			//session.save();
			
			// facet search
	    	String sqlStatement="";
	       
	          //Session session = resourceResolver.adaptTo(Session.class);
	        
	          QueryManager queryManager = session.getWorkspace().getQueryManager();
	            sqlStatement = "SELECT * FROM [nt:unstructured] AS n WHERE ISDESCENDANTNODE([/hello1])";
	            //sqlStatement = "select [jcr:path], [rep:facet(jcr:content/cq:tags)] from [cq:Page]"; //  +
	            //"where contains([jcr:title], 'oak')";
	          Query query = queryManager.createQuery(sqlStatement, "JCR-SQL2");
	          QueryResult result = query.execute();

	          RowIterator rows = result.getRows();
	            while(rows.hasNext()){
	                 Row row = rows.nextRow();
	                 System.out.println("row path:"+ row.getPath());
	                
	            }

	          // facet result
	          /*
	          FacetResult facetResult = new FacetResult(result);
	            Set<String> dimensions = facetResult.getDimensions(); // { "tags" }
	            //out.print("set size:"+dimensions.size());
	            List<FacetResult.Facet> facets = facetResult.getFacets("jcr:content/cq:tags");
	            for (FacetResult.Facet facet : facets) {
	                String label = facet.getLabel();
	                int count = facet.getCount();
	                System.out.println(label);
	                }
	           */ 
	        
			
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
    	
    	
    	
        
       
    	
    	
    	// Logout from repository
    	session.logout();
    	
    }
    
    // class to test adding a node
    private static void addNode() {
    	
    	// Create repository
    	Repository repo = new Jcr(new Oak()).createRepository();
    	
    	// Login to repository
    	Session session = null;
    	
    	try {
			session = repo.login(
			        new SimpleCredentials("admin", "admin".toCharArray()));
			Node root = session.getRootNode();
	        if (root.hasNode("hello")) {
	            Node hello = root.getNode("hello");
	            long count = hello.getProperty("count").getLong();
	            hello.setProperty("count", count + 1);
	            System.out.println("found the hello node, count = " + count);
	        } else {
	            System.out.println("creating the hello node");
	            root.addNode("hello").setProperty("count", 1);
	        }
	        session.save();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
        
    	// Logout from repository
    	session.logout();
    }

    

}
