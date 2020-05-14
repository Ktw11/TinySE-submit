package edu.hanyang.submit;

import java.io.*;
import java.util.*;

import org.apache.commons.lang3.tuple.MutableTriple;
import edu.hanyang.indexer.ExternalSort;

public class TinySEExternalSort implements ExternalSort {
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		/*
		 * blocksize * nblocks = available memory size
		 * nblocks = M
		 * */

		File dir = new File(tmpdir);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		RunManager rm = new RunManager();
		rm.dis = new DataInputStream(new BufferedInputStream(new FileInputStream(infile),blocksize));
		// 하나의 Run에 들어가는 최대 튜플 갯수
		int nElement = nblocks * blocksize / Integer.SIZE / 3;
		// 읽을 파일의 Byte 수
		int tot_input_size = rm.dis.available();
		// 마지막 Run에 들어가는 튜플 갯수
		int rest_nElement = (tot_input_size/ 12) % nElement;
		rm.initManager(infile,blocksize,nElement,tmpdir);

		int run_num = 0;
		try {
			/* Full Run */
			int loop_size = (tot_input_size/ 12)/nElement;
			for (int i = 0; i < loop_size; i++) {
				make_runs(rm,run_num);
				run_num++;
			}

			/* Rest Run */
			if(rest_nElement != 0) {
				rm.nElement = rest_nElement;
				make_runs(rm,run_num);
				run_num++;
			}

			/* Merge Start */
			DataInputStream run;
			int prevStepIdx = 0;
			while (true) {
				/* Last Step */
				if (run_num < nblocks) {
					ArrayList<DataInputStream> fileList = new ArrayList<>();
					for (int i = 0; i < run_num; i++) {
						run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + prevStepIdx + "_" + i + ".data"), blocksize));
						fileList.add(run);
					}
					_mergeSort(fileList, outfile);
					break;
				} else {
					int tot_iter = 1;
					int tmp_run = run_num;
					while (tmp_run > nblocks) {
						tmp_run = run_num % (nblocks - 1) == 0 ? run_num / (nblocks - 1) : (run_num / (nblocks - 1)) + 1;
						tot_iter++;
					}
					ArrayList<DataInputStream> fileList = new ArrayList<>();
					for (int kIter = 1; kIter < tot_iter; kIter++) {
						/* Full */
						int full_cnt = run_num / nblocks;
						for (int j = 0; j < full_cnt; j++) {
							for (int i = 0; i < nblocks; i++) {
								run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + (kIter - 1) + "_" + (j*nblocks + i) + ".data"), blocksize));
								fileList.add(run);
							}
							_mergeSort(fileList, tmpdir + File.separator + "run_" + kIter + "_" + j + ".data");
							fileList.clear();
						}

						/* Rest */
						if (run_num % nblocks > 0) {
							for (int i = 0; i < run_num % nblocks; i++) {
								run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + (kIter - 1) + "_" + (full_cnt*nblocks + i) + ".data"),blocksize));
								fileList.add(run);
							}
							_mergeSort(fileList, tmpdir + File.separator + "run_" + kIter + "_" + full_cnt + ".data");
							fileList.clear();
						}

						run_num = run_num % (nblocks-1) == 0 ? run_num / (nblocks-1) : (run_num / (nblocks-1)) + 1;
						if (run_num < nblocks){
							prevStepIdx = tot_iter - 1;
						}
					}
				}
			}
//			System.out.println("Merge done : " + (System.currentTimeMillis() - merge_stamp) + " msecs");
		} catch (EOFException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void make_runs(RunManager rm, int run_num) throws IOException {
		int nElement = rm.nElement;
		ArrayList<MutableTriple<Integer,Integer,Integer>> runs = new ArrayList<>(nElement);
		for (int j = 0; j < nElement; j++) {
			MutableTriple<Integer, Integer, Integer> run = new MutableTriple<>();
			rm.readFile();
			rm.replaceTuple(run);
			runs.add(run);
		}
		runs.sort(new TripleSort());
		/* init runs to file */
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(rm.tmpdir + File.separator + "run_0_" + run_num + ".data"),rm.blocksize));
		for(MutableTriple<Integer,Integer,Integer> tmp : runs){
			dout.writeInt(tmp.getLeft());
			dout.writeInt(tmp.getMiddle());
			dout.writeInt(tmp.getRight());
		}
		dout.close();
		runs.clear();
	}


	private void _mergeSort(ArrayList<DataInputStream> fileArr, String outfile) throws IOException {
		PriorityQueue<DataManager> pq = new PriorityQueue<>(new DataCmp());
		int arr_size = fileArr.size();
		int[] byte_size_idx = new int[arr_size];
		for(int i=0;i<arr_size;i++){
			byte_size_idx[i] = fileArr.get(i).available();
		}

		/* init PQ */
		for (DataInputStream f : fileArr) {
			try {
				DataManager dm = new DataManager(f.readInt(), f.readInt(), f.readInt(), fileArr.indexOf(f));
				pq.add(dm);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/* Now Only Consider Final Step*/
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outfile)));

		/* Write Next Step File */
		DataManager dm;
		DataInputStream tmp;
		while (!pq.isEmpty()) {
			try {
				dm = pq.poll();
				dout.writeInt(dm.tuple.getLeft());
				dout.writeInt(dm.tuple.getMiddle());
				dout.writeInt(dm.tuple.getRight());
				dout.flush();
				byte_size_idx[dm.index] -= 12;
				tmp = fileArr.get(dm.index);
				if (byte_size_idx[dm.index] > 0) {
					dm.setTuple(tmp.readInt(), tmp.readInt(), tmp.readInt());
					pq.add(dm);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		dout.close();
		pq.clear();
	}

	private static class DataManager {
		public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<>();
		int index;

		public DataManager(int left, int middle, int right, int idx){
			this.tuple.setLeft(left);
			this.tuple.setMiddle(middle);
			this.tuple.setRight(right);
			this.index = idx;
		}

		public void setTuple(int left, int middle, int right) {
			this.tuple.setLeft(left);
			this.tuple.setMiddle(middle);
			this.tuple.setRight(right);
		}

		public MutableTriple<Integer, Integer, Integer> getTuple() {
			return tuple;
		}

	}

	private static class RunManager{
		DataInputStream dis;
		int nElement;
		String path;
		String tmpdir;
		int blocksize;
		MutableTriple<Integer,Integer,Integer> tmp = new MutableTriple<>();
		public void initManager(String path, int blocksize, int nElement, String tmpdir) {
			this.nElement = nElement;
			this.path = path;
			this.blocksize = blocksize;
			this.tmpdir = tmpdir;
		}
		public RunManager(){
		}
		private void replaceTuple(MutableTriple<Integer,Integer,Integer> run) {
			run.setLeft(tmp.getLeft());
			run.setMiddle(tmp.getMiddle());
			run.setRight(tmp.getRight());
		}
		private void readFile() throws IOException {
			tmp.setLeft(dis.readInt());
			tmp.setMiddle(dis.readInt());
			tmp.setRight(dis.readInt());
		}
	}

	private static class DataCmp implements Comparator<DataManager>{
		@Override
		public int compare(DataManager o1, DataManager o2) {
			MutableTriple<Integer, Integer, Integer> t1 = o1.getTuple();
			MutableTriple<Integer, Integer, Integer> t2 = o2.getTuple();
			if (t1.getLeft() > t2.getLeft()) {
				return 1;
			} else if (t1.getLeft().equals(t2.getLeft())) {
				if (t1.getMiddle() > t2.getMiddle()) {
					return 1;
				} else if (t1.getMiddle().equals(t2.getMiddle())) {
					return Integer.compare(t1.getRight(), t2.getRight());
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		}
	}
	private static class TripleSort implements Comparator<MutableTriple<Integer,Integer,Integer>> {
		@Override
		public int compare(MutableTriple<Integer,Integer,Integer> t1, MutableTriple<Integer,Integer,Integer> t2) {
			if (t1.getLeft() > t2.getLeft()) {
				return 1;
			} else if (t1.getLeft().equals(t2.getLeft())) {
				if (t1.getMiddle() > t2.getMiddle()) {
					return 1;
				} else if (t1.getMiddle().equals(t2.getMiddle())) {
					return Integer.compare(t1.getRight(), t2.getRight());
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		}
	}

//	public static void main(String[] args) throws IOException {
//		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("./tmp/sorted.data")));
//		int cnt = 0;
//		while(dis.available() > 0){
//			int a= dis.readInt();
//			int b= dis.readInt();
//			int c= dis.readInt();
//			System.out.println(a + " "+b+" "+c);
//			cnt++;
//		}
//		System.out.println(cnt);
//		dis.close();
//	}
}