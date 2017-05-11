package ecp.bigdata.lab1.ex4t;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class Test extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }

        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche
        Job job = Job.getInstance(getConf());
        job.setJobName("WordCount");

        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(Test.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);

        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return job.waitForCompletion(true) ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        Test TestDriver = new Test();
        int res = ToolRunner.run(TestDriver, args);
        System.exit(res);
    }
}
