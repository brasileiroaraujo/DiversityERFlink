package parser;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Data.Attribute;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class TestTokens {

	public static void main(String[] args) {
		HashSet<Integer> tokens = new HashSet<Integer>();
		KeywordGenerator kw = new KeywordGeneratorImpl();
		Pattern p = Pattern.compile("[^\"a-zA-Z\\s0-9]");
		 
		
		Matcher m = p.matcher("");
		m.reset("\"Helsinki\"@fi");
		String standardString = m.replaceAll("");
		
		System.out.println(kw.generateKeyWordsHashCode(standardString));
		
		
		m = p.matcher("");
		m.reset("\"Helsinki\"@fin");
		standardString = m.replaceAll("");
		
		System.out.println(kw.generateKeyWordsHashCode(standardString));
		
		m = p.matcher("");
		m.reset("\"Helsinki\"");
		standardString = m.replaceAll("");
		
		System.out.println(kw.generateKeyWordsHashCode(standardString));
			
			

	}

}
