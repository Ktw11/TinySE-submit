package edu.hanyang.submit;

import java.io.IOException;

import edu.hanyang.indexer.DocumentCursor;
import edu.hanyang.indexer.PositionCursor;
import edu.hanyang.indexer.IntermediateList;
import edu.hanyang.indexer.IntermediatePositionalList;
import edu.hanyang.indexer.QueryPlanTree;
import edu.hanyang.indexer.QueryProcess;
import edu.hanyang.indexer.StatAPI;

public class TinySEQueryProcess implements QueryProcess {

	@Override
	public void op_and_w_pos(DocumentCursor op1, DocumentCursor op2, int shift, IntermediatePositionalList out)
			throws IOException {
		
		PositionCursor pc1, pc2;
		
		int doc1, doc2;
		int pos1,pos2;
		
		while (!op1.is_eol() && !op2.is_eol()) {
			doc1 = op1.get_docid();
			doc2 = op2.get_docid();
			
			if (doc1 < doc2) {
				op1.go_next();
			} else if (doc1 > doc2) {
				op2.go_next();
			} else {
				pc1 = op1.get_position_cursor();
				pc2 = op2.get_position_cursor();
				
				while (!pc1.is_eol() && !pc2.is_eol()) {
					pos1 = pc1.get_pos();
					pos2 = pc2.get_pos();
					
					if (pos1 + shift < pos2) {
						pc1.go_next();
					} else if (pos1 + shift > pos2) {
						pc2.go_next();
					} else {
						out.put_docid_and_pos(doc1, pos1);
						pc1.go_next();
						pc2.go_next();
					}
				}
				op1.go_next();
				op2.go_next();
			}
		}
		
	}
	
	@Override
	public void op_and_wo_pos(DocumentCursor op1, DocumentCursor op2, IntermediateList out) throws IOException {
		int doc1, doc2;
		
		while (!op1.is_eol() && !op2.is_eol()) {
			doc1 = op1.get_docid();
			doc2 = op2.get_docid();
			
			if (doc1 < doc2) {
				op1.go_next();
				
			} else if (doc1 > doc2) {
				op2.go_next();
				
			} else {
				out.put_docid(doc1);
				op1.go_next();
				op2.go_next();
			}
		}
		
	}

	@Override
	public QueryPlanTree parse_query(String query, StatAPI stat) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


}
