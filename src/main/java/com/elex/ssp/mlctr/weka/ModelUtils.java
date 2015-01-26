/*package com.elex.ssp.mlctr.weka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import weka.classifiers.Classifier;
import weka.classifiers.functions.Logistic;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.SparseInstance;
import weka.core.converters.ArffLoader;
import weka.core.converters.LibSVMLoader;
import weka.core.converters.Loader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;


public class ModelUtils {

	 private static String MODEL="D:\\20news-bydate\\weka.model";
	 private static String TRAINFILE="D:\\20news-bydate\\train.arff";
	 private static String TESTFILE="D:\\20news-bydate\\test.arff";
	 private static String DATATYPE="arff";
	 private static String PREDICTFILE;
	 private static String RESULT;
	 private static int FEATURES;
	 
	*//**
	 * @param args
	 * @throws Exception
	 * @throws IOException
	 *//*
	public static void main(String[] args) throws IOException, Exception {
		
		if(args.length==4){
			TRAINFILE=args[0];
			TESTFILE=args[1];
			MODEL=args[2];
			DATATYPE=args[3];
			train();
			
			//predict();
		}else{
			
			System.exit(-1);
		}
		

	}

	public static void train() throws Exception{
		System.out.println("Training Logistic...");
		
		Loader train;
		Loader test;
		Instances structure;
		Instances data;
		Instance current;
		int sum=0,correct=0,wrong=0;
		
		if(DATATYPE.equals("libsvm")){
			train = new LibSVMLoader();
			test = new LibSVMLoader();
			train.setSource(new File(TRAINFILE));
			test.setSource(new File(TESTFILE));	
			data = train.getDataSet();
			
			structure = train.getStructure();
			structure.setClassIndex(0);
			
			
			NumericToNominal filter = new NumericToNominal();
			filter.setOptions(new String[]{"-R","last"});
			filter.setInputFormat(data);			
			data = Filter.useFilter(data, filter);
			
			System.out.println("input format is libsvm");
		}else{
			train = new ArffLoader();
			test = new ArffLoader();
			data = train.getDataSet();
			structure = train.getStructure();
			structure.setClassIndex(structure.numAttributes() - 1);
			System.out.println("input format is arff");
		}				
		
		Logistic cls=new Logistic();
						 		
		int m_NumClasses = structure.classAttribute().numValues();
		String [] m_ClassNames = new String [m_NumClasses];
		
		for(int j =0;j<m_NumClasses;j++){
			m_ClassNames[j] = structure.classAttribute().value(j);
		}
		
		cls.buildClassifier(data); 
		
		while ((current = test.getNextInstance(structure)) != null) {
		    // classifier is of type SGD.
		    if(cls.classifyInstance(current)==current.classValue()){
		    	correct++;
		    }else{
		    	wrong++;
		    }
		    sum++;
		 }
			
		
		System.out.println("sum="+sum+",correct="+correct+",wrong="+wrong);
			
		// save model + header
	    Vector v = new Vector();
	    v.add(cls);
	    v.add(structure);
	    v.add(m_ClassNames);
	    SerializationHelper.write(MODEL, v);
		
		System.out.println("Training finished!");
		
	}
	
	
	public static void predict() throws Exception {
	    System.out.println("Predicting...");
	    
		
		// read model and header
	    Vector v = (Vector) SerializationHelper.read(MODEL);
	    Classifier cl = (Classifier) v.get(0);	    
	    Instances header = (Instances) v.get(1);
	    String [] m_ClassNames =(String[]) v.get(2);
		
		BufferedWriter result = new BufferedWriter(new FileWriter(new File(PREDICTFILE)));		
	    BufferedReader reader = new BufferedReader(new FileReader(new File(RESULT))); 
	    reader.readLine();
		String line = reader.readLine();
		String[] attributes,IDX_V_Pair;
		String gender;
		int size = 0;
		while(line != null){
			attributes = line.split("\t");
			size = attributes.length-1;
			SparseInstance curr = new SparseInstance(FEATURES);
			curr.setDataset(header);
			for(int i=0;i<size;i++){
				IDX_V_Pair = attributes[i].split(":");
				curr.setValue(Integer.parseInt(IDX_V_Pair[0]), Double.parseDouble(IDX_V_Pair[1]==null?"0":IDX_V_Pair[1]));
			}
			Double pred = cl.classifyInstance(curr);
			gender = m_ClassNames[pred>0.5D?1:0];
			curr.setClassValue(pred);
			result.write(attributes[0]+","+pred+","+gender+"\r\n");
			line = reader.readLine();
		}
		
		reader.close();
		result.close();

	    System.out.println("Predicting finished!");
	  }

}
*/