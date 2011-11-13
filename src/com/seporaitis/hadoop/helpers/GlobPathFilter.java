package com.seporaitis.hadoop.helpers;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A class for filtering input files using a 'glob' type filtering.
 * 
 * Implementation credit:
 * http://stackoverflow.com/questions/1247772/is-there-an-equivalent-of-java-util-regex-for-glob-type-patterns
 */
public class GlobPathFilter implements PathFilter {
    
    private String regex = "";
    
    public GlobPathFilter(Configuration conf) {
        String glob = conf.getRaw("mapred.input.pathFilter.glob");
        if(glob == null) {
            throw new RuntimeException("You should set 'mapred.input.pathFilter.glob'.");
        }
        
        regex = "^";
        for(int i = 0; i < glob.length(); ++i) {
            final char c = glob.charAt(i);
            switch(c) {
                case '*': regex += ".*"; break;
                case '?': regex += '.'; break;
                case '.': regex += "\\."; break;
                case '\\': regex += "\\\\"; break;
                default: regex += c;
            }
        }
        regex += "$";
    }

    @Override
    public boolean accept(Path path) {
        if(Pattern.matches(regex, path.toString())) {
            return true;
        }
        
        return false;
    }

}
