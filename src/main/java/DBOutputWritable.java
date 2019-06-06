import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{

	private String startingPhrase;
	private String followingWord;
	private int count;
	
	public DBOutputWritable(String starting_prhase, String following_word, int count) {
		this.startingPhrase = starting_prhase;
		this.followingWord = following_word;
		this.count= count;
	}

	public void readFields(ResultSet arg0) throws SQLException {
		//Read from input, column start from 1, ResultSet is an interface: get"Format", update"Format", add,
		this.startingPhrase = arg0.getString(1);
		this.followingWord = arg0.getString(2);
		this.count = arg0.getInt(3);
	}

	public void write(PreparedStatement arg0) throws SQLException {
		//Write into arg0, column start from 1, similar to ResultSet
		arg0.setString(1, startingPhrase);
		arg0.setString(2, followingWord);
		arg0.setInt(3, count);
	}

}
