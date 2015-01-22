package com.elex.ssp.mlctr.weka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import weka.classifiers.Classifier;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SGD;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.SparseInstance;
import weka.core.converters.ArffLoader;
import weka.core.converters.LibSVMLoader;
import weka.core.converters.Loader;


public class ModelUtils {

	 private static String MODEL="D:\\20news-bydate\\weka.model";
	 private static String TRAINFILE="D:\\20news-bydate\\train.arff";
	 private static String TESTFILE="D:\\20news-bydate\\test.arff";
	 private static String DATATYPE="arff";
	 private static String PREDICTFILE;
	 private static String RESULT;
	 private static int FEATURES;
	 
	/**
	 * @param args
	 * @throws Exception
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, Exception {
		
		if(args.length==5){
			TRAINFILE=args[0];
			TESTFILE=args[1];
			MODEL=args[2];
			DATATYPE=args[3];
			if(args[4].equals("logistic")){
				trainLogistic();
			}else{
				trainSGDIncrement();
			}
			
			//predict();
		}else{
			
			System.exit(-1);
		}
		

	}

	public static void trainSGDIncrement() throws Exception{
		System.out.println("Training...");
		
		Loader train;
		Loader test;
		if(DATATYPE.equals("libsvm")){
			train = new LibSVMLoader();
			test = new LibSVMLoader();
			
		}else{
			train = new ArffLoader();
			test = new ArffLoader();
		}
		
		train.setSource(new File(TRAINFILE));
		
		SGD cls=new SGD();
		Instances structure = train.getStructure(); 
		structure.setClassIndex(structure.numAttributes() - 1); 
		Instance current;
		int m_NumClasses = structure.classAttribute().numValues();
		String [] m_ClassNames = new String [m_NumClasses];
		
		for(int j =0;j<m_NumClasses;j++){
			m_ClassNames[j] = structure.classAttribute().value(j);
		}
		
		cls.buildClassifier(structure); 
		while ((current = train.getNextInstance(structure)) != null) {
		    // classifier is of type SGD.
		    cls.updateClassifier(current);
		 }
		
		
		int sum=0,correct=0,wrong=0;
		test.setSource(new File(TESTFILE));
		structure = test.getStructure(); 
		if(DATATYPE.equals("libsvm")){
			structure.setClassIndex(0);
		}else{
			structure.setClassIndex(structure.numAttributes() - 1);
		}
		
		
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

	public static void trainLogistic() throws Exception{
		System.out.println("Training...");
		Loader train;
		Loader test;
		if(DATATYPE.equals("libsvm")){
			train = new LibSVMLoader();
			test = new LibSVMLoader();
			
		}else{
			train = new ArffLoader();
			test = new ArffLoader();
		}
		
		train.setSource(new File(TRAINFILE));
		
		Logistic cls=new Logistic();
		cls.setOptions(new String[]{"-R","0.00000001","-M","-1"});
		
		Instances structure = train.getStructure(); 
		structure.setClassIndex(structure.numAttributes() - 1); 
		Instance current;
		int m_NumClasses = structure.classAttribute().numValues();
		String [] m_ClassNames = new String [m_NumClasses];
		
		for(int j =0;j<m_NumClasses;j++){
			m_ClassNames[j] = structure.classAttribute().value(j);
		}
		
		Instances data = train.getDataSet();
		
		if(DATATYPE.equals("libsvm")){
			data.setClassIndex(0); 
		}else{
			data.setClassIndex(data.numAttributes() - 1); 
		}
		
		cls.buildClassifier(data);
		
		
		int sum=0,correct=0,wrong=0;
		
		test.setSource(new File(TESTFILE));
		structure = test.getStructure(); 
		structure.setClassIndex(structure.numAttributes() - 1);
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
