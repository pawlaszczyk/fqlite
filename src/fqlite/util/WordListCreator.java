package fqlite.util;

import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.ui.NodeObject;
import javafx.scene.control.TreeItem;

import java.util.List;
import java.util.Objects;

/**
 * Simple helper class. Creates a wordlist.
 *
 * @author pawel
 */
public class WordListCreator {

    public static void updateWordList(TreeItem<NodeObject> node, List<String> WORD_LIST){

        // add all tablenames and column names to wordlist
        for (TableDescriptor td : node.getValue().job.headers)
        {
            if(td == null)
                continue;

            WORD_LIST.add(td.tblname);

            for (String cln : td.columnnames){

                WORD_LIST.add(cln);

            }
        }

        // add all index names and column names to wordlist
        for (IndexDescriptor id : node.getValue().job.indices)
        {
            WORD_LIST.add(id.tblname);

            for (String cln : id.columnnames){

                WORD_LIST.add(cln);

            }

        }

        WORD_LIST.removeIf(Objects::isNull);
        WORD_LIST.sort(String.CASE_INSENSITIVE_ORDER);
    }

}
