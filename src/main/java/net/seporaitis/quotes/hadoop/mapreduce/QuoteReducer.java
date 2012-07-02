/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.quotes.hadoop.mapreduce;

import java.io.IOException;
import java.io.StringWriter;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFWriter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.seporaitis.quotes.hadoop.input.ModelWritable;


/**
 * Quote reducer.
 *
 * Merges extracted quotes and serializes to Turtle.
 */
public class QuoteReducer extends Reducer<Text, ModelWritable, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<ModelWritable> listOfValues,
                       QuoteReducer.Context context) throws IOException,
                                                            InterruptedException {
        Model model = ModelFactory.createDefaultModel();

        for (ModelWritable value : listOfValues) {
            model = model.union(value.getModel());
        }

        RDFWriter writer = model.getWriter("TURTLE");
        writer.setProperty("tab", "4");
        writer.setProperty("relativeURIs", "same-document,relative");
        writer.setProperty("useTripleQuotedStrings", "true");

        StringWriter output = new StringWriter();
        writer.write(model, output, "http://localhost/");

        result.set(output.toString());
        context.write(null, result);
    }

}
