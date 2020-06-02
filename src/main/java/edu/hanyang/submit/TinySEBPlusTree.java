package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

class Node {
	int isLeaf;
	int amountOfKeys;
	int position;
	int parent_position;
	
	ArrayList<Integer> key = new ArrayList<Integer>();
	ArrayList<Integer> value = new ArrayList<Integer>();
	
	public Node(){
		this.amountOfKeys = 0;
		this.isLeaf = 1;
		this.position = 0;
		this.parent_position = -1;
	}
}

public class TinySEBPlusTree implements BPlusTree {
	int blocksize;
	int root_pos;
	int max_keys;
	
	RandomAccessFile raf;

	@Override
	public void open(String metapath, String savepath, int blocksize, int nblocks) {
		this.blocksize = blocksize;
		
		this.max_keys = (blocksize / (Integer.SIZE / 4)) - 2;		
		
		try{
			this.raf = new RandomAccessFile(savepath, "rw");
			
			if (raf.length() == 0) {
				this.root_pos = 4;
				this.raf.writeInt(this.root_pos);
			}
			else{
				this.root_pos = this.raf.readInt();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	@Override
	public int search(int key) {
		return _search(key, this.root_pos);
	}
	
	/* Recursive Search */
	public int _search(int key, int root){
		Node cur = readNodeData(root);
		ArrayList<Integer> keys = cur.key;
		ArrayList<Integer> vals = cur.value;
		
		if(cur.isLeaf == 1){
			for(int i=0; i < cur.amountOfKeys; i++){
				if(keys.get(i) == key){
					return vals.get(i);
				}
			}
			
			return 0; /* not found */
			
		} else {
			for(int i = 0; i < cur.amountOfKeys; i++) {
				if(key < keys.get(i)) {
					return _search(key, vals.get(i)); /* to left */
				}
			}
			
			return _search(key, vals.get(cur.amountOfKeys)); /* to right */
		}
	}
	
	@Override
	public void insert(int key, int value) {
		Node cur = readNodeData(this.root_pos);
		
		ArrayList<Integer> keys = cur.key;
		ArrayList<Integer> vals = cur.value;
		
		if (cur.isLeaf == 1) { /* if root is leaf (never splited) */
			if(cur.amountOfKeys == 0) { /* init */
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.amountOfKeys++;
				writeNodeData(cur);
				
			} else if (key < keys.get(0)) { /* Add on Front */
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.amountOfKeys++;
				
				if (cur.amountOfKeys > this.max_keys){
					split(cur);
				} else{
					writeNodeData(cur);
				}
			} else if (key > keys.get(cur.amountOfKeys-1)) { /* Add on Last */
				cur.key.add(cur.amountOfKeys, key);
				cur.value.add(cur.amountOfKeys, value);
				cur.amountOfKeys++;
				
				if(cur.amountOfKeys > this.max_keys) {
					split(cur);
				} else{
					writeNodeData(cur);
				}
				
			} else { /* Add on Middle of the Node */
				for(int i = 0; i < cur.amountOfKeys - 1; i++){
					if(key > keys.get(i) && key < keys.get(i+1)){
						cur.key.add(i+1, key);
						cur.value.add(i+1, value);
						cur.amountOfKeys++;
						break;
					}
				}
				
				if (cur.amountOfKeys > this.max_keys) {
					split(cur);
				} else{
					writeNodeData(cur);
				}
			}
		} else { /* if root is non-leaf node */
			while(true) {
				if(key < keys.get(0)){ /* To Left Child Node */
					cur = readNodeData(vals.get(0));
				} else if(key > keys.get(cur.amountOfKeys-1)){ /* To Right Child Node */
					cur = readNodeData(vals.get(cur.amountOfKeys));
				} else {
					for(int i = 0; i < cur.amountOfKeys - 1; i++){
						/* Found Position */
						if(key > keys.get(i) && key < keys.get(i+1)){
							cur = readNodeData(vals.get(i+1));
							break;
						}
					}
				}
				
				keys = cur.key;
				vals = cur.value;
				
				if (cur.isLeaf == 1) {
					if(key < keys.get(0)){ /* Add on First */
						cur.key.add(0, key);
						cur.value.add(0, value);
						cur.amountOfKeys++;
						
						if (cur.amountOfKeys > this.max_keys){
							split(cur);
						} else {
							writeNodeData(cur);
						}
					} else if (key > keys.get(cur.amountOfKeys-1)) { /* Add on Last */
						cur.key.add(cur.amountOfKeys, key);
						cur.value.add(cur.amountOfKeys, value);
						cur.amountOfKeys++;
						if (cur.amountOfKeys > this.max_keys){
							split(cur);
						} else {
							writeNodeData(cur);
						}
					} else { /* Add on Middle */
						for (int i = 0; i < cur.amountOfKeys - 1; i++) {
							/* Found */
							if (key > keys.get(i) && key < keys.get(i+1)) {
								cur.key.add(i+1, key);
								cur.value.add(i+1, value);
								cur.amountOfKeys++;
								break;
							}
						}
						
						if(cur.amountOfKeys > this.max_keys){
							split(cur);
						} else {
							writeNodeData(cur);
						}
					}
					
					break;
				}
			}
		}
		
		keys = null;
		vals = null;
	}

	
	public Node readNodeData(int pos) {
		Node new_node = new Node();
		
		ByteBuffer buffer;
		byte[] buf = new byte[this.blocksize];		
		
		int key, val;
		
		try{
			if(this.raf.length() == 4){
				new_node.position = 4;
				return new_node;
			}
			
			this.raf.seek(pos);
			this.raf.read(buf);
			
			buffer = ByteBuffer.wrap(buf);
			new_node.isLeaf = buffer.getInt();
			new_node.parent_position = buffer.getInt();
			new_node.amountOfKeys = buffer.getInt();
			new_node.position = pos;
			
			for(int i = 0; i < new_node.amountOfKeys; i++){
				key = buffer.getInt();
				new_node.key.add(key);
				val = buffer.getInt();
				new_node.value.add(val);
			}
			
			/* Non-Leaf : val = key + 1 */
			if(new_node.isLeaf == 0) {  
				new_node.value.add(buffer.getInt());
			}
			
		} catch(Exception e){
			e.printStackTrace();
		}
		
		return new_node;
	}
	
	public void writeNodeData(Node node) {
		byte[] buf = new byte[this.blocksize];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		
		int amountOfKeys = node.amountOfKeys;
		ArrayList<Integer> keys = node.key;
		ArrayList<Integer> vals = node.value;
		
		
		try{
			buffer.putInt(node.isLeaf);
			buffer.putInt(node.parent_position);
			buffer.putInt(amountOfKeys);
			
			for(int i=0; i < amountOfKeys; i++){
				buffer.putInt(keys.get(i));
				buffer.putInt(vals.get(i));
			}
			
			/* Non-Leaf */
			if(node.isLeaf == 0){ 
				buffer.putInt(vals.get(amountOfKeys));
			}

			this.raf.seek(node.position);
			this.raf.write(buf);
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		keys = null;
		vals = null;
	}
	
	public Node split(Node node) {
		Node parent;
		Node leftNode = new Node();
		Node rightNode = new Node();
		
		int parent_idx = node.parent_position; // new_parent_idx = now_parent_idx
		int is_new_root = 0;
		
		ArrayList<Integer> keys = node.key;
		ArrayList<Integer> vals = node.value;
		
		if(node.parent_position == -1) { /* Root */
			parent = new Node();
			parent.isLeaf = 0;
			is_new_root = 1;
			
		} else {
			parent = readNodeData(parent_idx);
		}
						 
		try{
			if(is_new_root == 1){ /* Root Case */
				parent.position = (int)this.raf.length() + this.blocksize;
				this.root_pos = parent.position;
			}
			
			if(node.isLeaf == 1) {
				parent_idx = node.amountOfKeys % 2 == 0 ? node.amountOfKeys / 2 : node.amountOfKeys / 2 + 1;

				/* TO LETF*/
				for(int i = 0; i < parent_idx; i++) {
					leftNode.key.add(keys.get(i));
					leftNode.amountOfKeys++;
					leftNode.value.add(vals.get(i));
				}
				
				leftNode.position = node.position;
				leftNode.parent_position = parent.position;
				writeNodeData(leftNode);
				
				/* TO RIGHT */
				for(int i = parent_idx; i < node.amountOfKeys; i++){
					rightNode.key.add(keys.get(i));
					rightNode.amountOfKeys++;
					rightNode.value.add(vals.get(i));
				}
				
				rightNode.position = (int)this.raf.length();
				rightNode.parent_position = parent.position;
				writeNodeData(rightNode);
				
				if(parent.amountOfKeys == 0 || parent.key.get(0) > keys.get(parent_idx)) {
					parent.key.add(0,keys.get(parent_idx));
					parent.amountOfKeys++;
					
					if(parent.amountOfKeys == 1) {
						parent.value.add(0, leftNode.position);
					} else {
						parent.value.set(0, leftNode.position);
					}
					
					parent.value.add(1, rightNode.position);
					
				} else if (parent.key.get(parent.amountOfKeys-1) < keys.get(parent_idx)) {
					parent.key.add(keys.get(parent_idx));
					parent.amountOfKeys++;
					parent.value.set(parent.amountOfKeys - 1, leftNode.position);
					parent.value.add(rightNode.position);
				} else {
					for(int i = 1; i < parent.amountOfKeys; i++){
						if(parent.key.get(i) > keys.get(parent_idx)){
							parent.key.add(i,keys.get(parent_idx));
							parent.amountOfKeys++;
							parent.value.set(i, leftNode.position);
							parent.value.add(i+1, rightNode.position);
							break;
						}
					}
				}
				
				if(parent.amountOfKeys > this.max_keys) {
					split(parent);
				} else {
					if(is_new_root == 1){
						this.raf.seek(0);
						this.raf.writeInt(parent.position);
					}
					
					writeNodeData(parent);
				}
				
			} else { /* Non-Leaf Node Split */
				leftNode.isLeaf = 0;
				rightNode.isLeaf = 0;
				parent_idx = node.amountOfKeys/2;
				
				for(int i=0; i < parent_idx; i++){
					leftNode.key.add(keys.get(i));
					leftNode.amountOfKeys++;
					leftNode.value.add(vals.get(i));
				}
				
				leftNode.value.add(vals.get(parent_idx));
				leftNode.position = node.position;
				leftNode.parent_position = parent.position;
				
				writeNodeData(leftNode);
				for(int i = parent_idx + 1; i < node.amountOfKeys; i++){
					rightNode.key.add(keys.get(i));
					rightNode.amountOfKeys++;
					rightNode.value.add(vals.get(i));
				}
				
				rightNode.value.add(vals.get(node.amountOfKeys));
				rightNode.position = (int)this.raf.length();
				rightNode.parent_position= parent.position;
				writeNodeData(rightNode);
				
				for(int i = 0; i <= rightNode.amountOfKeys; i++){
					Node temp = readNodeData(rightNode.value.get(i));
					temp.parent_position = rightNode.position;
					writeNodeData(temp);
				}
				
				if (parent.amountOfKeys == 0 || parent.key.get(0) > keys.get(parent_idx)) {
					parent.key.add(0,keys.get(parent_idx));
					parent.amountOfKeys++;
					
					if(parent.amountOfKeys == 1) {
						parent.value.add(0, leftNode.position);
					} else {
						parent.value.set(0, leftNode.position);
					}
					
					parent.value.add(1, rightNode.position);
					
				} else if(parent.key.get(parent.amountOfKeys-1) < keys.get(parent_idx)) {
					parent.key.add(keys.get(parent_idx));
					parent.amountOfKeys++;
					parent.value.set(parent.amountOfKeys-1, leftNode.position);
					parent.value.add(rightNode.position);
				} else {
					for(int i = 1; i < parent.amountOfKeys; i++){
						if(parent.key.get(i) > keys.get(parent_idx)){
							parent.key.add(i,keys.get(parent_idx));
							parent.amountOfKeys++;
							parent.value.set(i, leftNode.position);
							parent.value.add(i+1, rightNode.position);
							break;
						}
					}
				}
				
				if(parent.amountOfKeys > this.max_keys) {
					split(parent);
				} else {
					if(is_new_root == 1) {
						this.raf.seek(0);
						this.raf.writeInt(parent.position);
					}
					writeNodeData(parent);
				}
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		keys = null;
		vals = null;
		
		return parent;
	}
	
	@Override
	public void close() {
		try{
			this.raf.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}

