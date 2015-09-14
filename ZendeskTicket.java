package edu.umd.cloud9.collection.zendesk;

import info.bliki.wiki.filter.PlainTextConverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * A page from Wikipedia.
 * 
 * @author Jimmy Lin
 */
public class ZendeskTicket extends Indexable {

    /**
     * Start delimiter of the page, which is &lt;<code>page</code>&gt;.
     */
    public static final String XML_START_TAG = "<ticket>";

    /**
     * End delimiter of the page, which is &lt;<code>/page</code>&gt;.
     */
    public static final String XML_END_TAG = "</ticket>";

    private String mPage;
    private String mTitle;
    private String mId;
    private int mTextStart;
    private int mTextEnd;

    /**
     * Deserializes this object.
     */
    public void write(DataOutput out) throws IOException {
        byte[] bytes = mPage.getBytes();
        WritableUtils.writeVInt(out, bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    /**
     * Serializes this object.
     */
    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        ZendeskTicket.readPage(this, new String(bytes));
    }

    /**
     * Returns the article title (i.e., the docid).
     */
    public String getDocid() {
        return mId;
    }

    /**
     * Returns the contents of this page (title + text).
     */
    public String getContent() {
        String s = getWikiMarkup();

        // The way the some entities are encoded, we have to unescape twice.
        s = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(s));

        return s;
    }

    /**
     * Returns the raw XML of this page.
     */
    public String getRawXML() {
        return mPage;
    }

    /**
     * Returns the text of this page.
     */
    public String getWikiMarkup() {
        if (mTextStart == -1)
            return null;
        int StartIndex = 0;
        int StopIndex = 0;
        String outstring = "";
        
        //get comments
        String comments = mPage.substring(mTextStart + 23, mTextEnd);
        //System.out.println("comments = " + comments);

        //while there are still comments to grab
        while(StartIndex != -1){
            //get the index start of the comment
            StartIndex = comments.indexOf("<comment>",0);
     	    //System.out.println("StartIndex = " + StartIndex);       
            //get author
            int AuthorIndexStart = comments.indexOf("<author-id type=\"integer\">",StartIndex);
            int AuthorIndexStop = comments.indexOf("</author-id>",AuthorIndexStart);
            String Author = comments.substring(AuthorIndexStart + 26,AuthorIndexStop);
            //System.out.println("Author = " + Author);

            
            //get content of comment
            int ValueIndexStart = comments.indexOf("<value>",AuthorIndexStop);
            int ValueIndexStop = comments.indexOf("</value>",ValueIndexStart);
            String CommentVal = comments.substring(ValueIndexStart + 7,ValueIndexStop);
	   //System.out.println("CommentVal = " + CommentVal);
            
            //add author and comment to outstring
            outstring += "Author: " + Author + " \n " + CommentVal + " \n ";
            
            //get end of comment
            StopIndex = comments.indexOf("</comment>",ValueIndexStop);
            
            comments = comments.substring(StopIndex + 10, comments.length());
            StartIndex = comments.indexOf("<comment>",0);

            
        }
        return outstring;
        
        
    }

    /**
     * Returns the title of this page.
     */
    public String getTitle() {
        return mTitle;
    }

    /**
     * Checks to see if this page is an empty page. A <code>WikipediaPage</code>
     * is either an article, a disambiguation page, a redirect page, or an empty
     * page.
     * 
     * @return <code>true</code> if this page is an empty page
     */
    public boolean isEmpty() {
        return mTextStart == -1;
    }

    /**
     * Reads a raw XML string into a <code>WikipediaPage</code> object.
     * 
     * @param page
     *            the <code>WikipediaPage</code> object
     * @param s
     *            raw XML string
     */
    public static void readPage(ZendeskTicket page, String s) {
        page.mPage = s;
        //System.out.println(s);
        // parse out title
        int start = s.indexOf("<subject>");
        int end = s.indexOf("</subject>", start);
	//System.out.println("Start = " + start + " and End = " + end);
	if (start + 9 == end){
		page.mTitle = "";
	} else if (s.indexOf("<subject nil=\"true\"/>") != -1){
		page.mTitle = "";
	} else {
	        page.mTitle = StringEscapeUtils.unescapeHtml(s.substring(start + 9, end));
	}
	System.out.println("page.mTitle = " + page.mTitle);

        start = s.indexOf("<nice-id type=\"integer\">");
        end = s.indexOf("</nice-id>");
        page.mId = s.substring(start + 24, end);
        //System.out.println("page.mId = " + page.mId);


        // parse out actual text of article
        page.mTextStart = s.indexOf("<comments type=\"array\">");
        page.mTextEnd = s.indexOf("</comments>", page.mTextStart);
    }
}

