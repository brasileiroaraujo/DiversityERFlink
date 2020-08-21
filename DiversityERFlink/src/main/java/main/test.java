package main;

import java.util.TreeSet;

import org.apache.commons.collections.list.TreeList;

import Data.EntityProfile;

public class test {

	public static void main(String[] args) {
		EntityProfile e1 = new EntityProfile("teste1");
		e1.setDiversity(0.0);
		e1.setSimilarity(0.2);
		EntityProfile e2 = new EntityProfile("teste2");
		e2.setDiversity(0.6);
		e2.setSimilarity(0.6);
		EntityProfile e3 = new EntityProfile("teste3");
		e3.setDiversity(0.6);
		e3.setSimilarity(0.9);
		
		long init1 = System.currentTimeMillis();
		TreeSet myTreeSet = new TreeSet();
		myTreeSet.add(e1);
		myTreeSet.add(e2);
		myTreeSet.add(e3);
		long end1 = System.currentTimeMillis();
		System.out.println(myTreeSet);
		System.out.println(end1-init1);
		
		long init2 = System.currentTimeMillis();
		TreeList treeList = new TreeList();
		myTreeSet.add(e1);
		myTreeSet.add(e2);
		myTreeSet.add(e3);
		long end2 = System.currentTimeMillis();
		System.out.println(myTreeSet);
		System.out.println(end2-init2);

	}

}
