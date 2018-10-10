package com.headwire.app;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;


/**
 * Created by lq on 10/10/2018.
 */
public class CreateRepository {

    public CreateRepository() {
        
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
        
    	// Todo: add nodes
    	
    	// Logout from repository
    	session.logout();

    	
    	
    }

    

}
