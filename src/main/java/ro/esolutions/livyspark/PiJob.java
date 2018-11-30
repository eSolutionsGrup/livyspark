package ro.esolutions.livyspark;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

import java.util.ArrayList;
import java.util.List;

public class PiJob implements Job<Integer> {

    private final int samples;

    public PiJob(int samples) {
        this.samples = samples;
    }

    public Integer call(JobContext ctx) throws Exception {
        List<Integer> sampleList = new ArrayList<Integer>();
        for (int i = 0; i < samples; i++) {
            sampleList.add(i + 1);
        }

        return ctx.sc().parallelize(sampleList).reduce((a,b) -> a + b);

//        return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
    }



}