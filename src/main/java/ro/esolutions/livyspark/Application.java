package ro.esolutions.livyspark;

import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class Application {

    private final static Logger LOGGER = Logger.getLogger(Application.class.getName());

    public static void main(String[] args) throws URISyntaxException, IOException, ExecutionException, InterruptedException {
        if (args.length != 2) {
            LOGGER.info("Usage: PiJob <livy url> <slices>");
            System.exit(-1);
        }

        final long startTime = System.currentTimeMillis();

        final LivyClient client = new LivyClientBuilder()
                .setURI(new URI(args[0]))
                .build();

        try {
            System.out.println("Uploading livyspark jar to the SparkContext...");


//            client.uploadJar(new File("/home/lucian/workspace/spark/livyspark/target/livyspark-1.0-SNAPSHOT-jar-with-dependencies.jar"));

            client.addJar(new URI("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/livyspark-1.0-SNAPSHOT.jar"));

            System.out.println("PROCESS PI " + args[1]);

            final int slices = Integer.parseInt(args[1]);
            JobHandle<Integer> job = client.submit(new PiJob(slices));
            job.addListener(new JobHandle.Listener<Integer>() {
                @Override
                public void onJobQueued(JobHandle<Integer> jobHandle) {
                    System.out.println("onJobQueued");
                }

                @Override
                public void onJobStarted(JobHandle<Integer> jobHandle) {
                    System.out.println("onJobStarted");
                }

                @Override
                public void onJobCancelled(JobHandle<Integer> jobHandle) {
                    System.out.println("onJobCancelled");
                }

                @Override
                public void onJobFailed(JobHandle<Integer> jobHandle, Throwable throwable) {
                    System.out.println("onJobFailed");
                }

                @Override
                public void onJobSucceeded(JobHandle<Integer> jobHandle, Integer integer) {
                    System.out.println("onJobSucceeded");
                }
            });

            Integer pi = job.get();

            System.out.println("Pi is roughly " + pi);

            final long endTime = System.currentTimeMillis();

            System.out.println("Total execution time: " + (endTime - startTime));
        } finally {
            client.stop(true);
        }

    }
}
