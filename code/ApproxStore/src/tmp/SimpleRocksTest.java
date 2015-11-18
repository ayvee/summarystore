package tmp;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.CvType;
import org.opencv.core.Scalar;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
//import org.opencv.highgui.Highgui;
import org.opencv.videoio.*;
import org.opencv.videoio.VideoCapture;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

import java.awt.image.BufferedImage;

public class SimpleRocksTest {
	static {
		  RocksDB.loadLibrary();  
		  System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}
	
	public static void testOpenCV() {
		System.out.println("Welcome to OpenCV " + Core.VERSION);
	    Mat m = new Mat(5, 10, CvType.CV_8UC1, new Scalar(0));
	    System.out.println("OpenCV Mat: " + m);
	    Mat mr1 = m.row(1);
	    mr1.setTo(new Scalar(1));
	    Mat mc5 = m.col(5);
	    mc5.setTo(new Scalar(5));
	    System.out.println("OpenCV Mat data:\n" + m.dump());
	}
	
	public void detectFaceDemo() {
		
		System.out.println("\nRunning DetectFaceDemo");

	    // Create a face detector from the cascade file in the resources
	    // directory.
		//String className = this.getClass().getResource(this.getClass().getSimpleName().toString());
	    //String testPath = this.getClass().getResource(classNameng()+".class");  
	    //System.out.println("Current Running Location is :"+className); 
		//System.out.println("r: " + SimpleRocksTest.class.getClassLoader().getResource("").getPath() + "\n");
		//System.out.println("\nRunning DetectFaceDemo");

	    //CascadeClassifier faceDetector = new CascadeClassifier(getClass().getResource("/lbpcascade_frontalface.xml").getPath());
	    //Mat image = Imgcodecs.imread(getClass().getResource("/lena.png").getPath());
		
		
		
		CascadeClassifier faceDetector = new CascadeClassifier("/home/n.agrawal1/ss/res/lbpcascade_frontalface.xml");
	    VideoCapture vc  = new VideoCapture("/home/n.agrawal1/ss/res/cvpr.mpg");
		//Mat image = Imgcodecs.imread("/home/n.agrawal1/ss/res/lena.png");

	    Mat image = new Mat();
	    if(vc.isOpened()) {
	    	while(true) {
	    		while (vc.read(image)){
	    			/*
                    BufferedImage image = t.MatToBufferedImage(frame);
                    t.window(image, "Original Image", 0, 0);
                    t.window(t.grayscale(image), "Processed Image", 40, 60);
                    t.window(t.loadImage("ImageName"), "Image loaded", 0, 0);
					*/

	    		    // Detect faces in the image.
	    		    // MatOfRect is a special container class for Rect.
	    		    MatOfRect faceDetections = new MatOfRect();
	    		    faceDetector.detectMultiScale(image, faceDetections);

	    		    System.out.println(String.format("Detected %s faces", faceDetections.toArray().length));

	    		    // Draw a bounding box around each face.
	    		    for (Rect rect : faceDetections.toArray()) {
	    		        Imgproc.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0));
	    		    }

	    		    // Save the visualized detection.
	    		    String filename = "faceDetection.png";
	    		    System.out.println(String.format("Writing %s", filename));
	    		    Imgcodecs.imwrite(filename, image);
	    	    }
	    		
	    	}
	    	
	    } else {
	    	System.out.println("Video file not opened");
	    }
	    
	    vc.release();
	    
	}
	
	public static void main(String[] args) {
		  // the Options class contains a set of configurable DB options
		  // that determines the behavior of a database.
		  Options options = new Options().setCreateIfMissing(true);
		  RocksDB db = null;
		  
		  byte[] key1 = {1};
		  byte[] key2 = {2};
	
		  testOpenCV();
		  SimpleRocksTest s = new SimpleRocksTest();
		  s.detectFaceDemo();
		  
		  try {
		    // a factory method that returns a RocksDB instance
		    System.out.println("Hello World3");
			db = RocksDB.open(options, "/tmp/testdb");
			assert(db!=null);

			db.put("mine".getBytes(), "yours".getBytes());		
			byte[] val = db.get(key1);
			if (val!=null) {
				db.put(key2, val);
				byte[] val2 = db.get(key2);
				System.out.println("Value 2  = " + val2);
			} else {
				db.put(key2, "hellow".getBytes());
				byte[] val2 = db.get(key2);
				System.out.println("Value 2  = " + val2);
			}
		    
			System.out.println("Value  = " + new String(db.get("mine".getBytes())));

		    if (db != null) 
		    	db.close();
		    options.dispose();
		    
		  } catch (RocksDBException e) {
			  e.printStackTrace();
		  }
		  
		  
	}
}
