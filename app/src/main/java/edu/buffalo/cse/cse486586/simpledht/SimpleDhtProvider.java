package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteOpenHelper;

import android.database.sqlite.SQLiteDatabase;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.net.Uri;

import java.io.BufferedReader;
import java.io.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;


public class SimpleDhtProvider extends ContentProvider {

    public static String pred, succ , curr;
    public static String myPort, predHash, succHash;
    static final int SERVER_PORT = 10000;
    String portStr;
    ArrayList<String> nodes = new ArrayList<String>();


    final Uri myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public static final String TABLE_NAME = "Msgs";
    public static final String DATABASE_NAME = "SimpleDht.db";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final int DATABASE_VERSION = 1;
    public static int z=0;
    public class MyDBHelper extends SQLiteOpenHelper {

        public MyDBHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        public void onCreate(SQLiteDatabase db) {
            //CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT);
            db.execSQL("CREATE TABLE " + TABLE_NAME + "( " + KEY + " TEXT PRIMARY KEY, " + VALUE + " TEXT);");
        }

        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.v("Upgrading", TABLE_NAME);
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
            onCreate(db);
        }
    }
    private MyDBHelper dbHelp;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        SQLiteDatabase db = dbHelp.getWritableDatabase();
        if(selection.equals("*") || selection.equals("@")){
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
        }
        db.delete(TABLE_NAME,"( key = '"+selection+"' )",selectionArgs);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String hashOfKey = genHash(key);

            Log.v("hashOfKey",hashOfKey);
            myPort = genHash(portStr);

            if(pred!=null)
                predHash = genHash(pred);
            if(succ!=null)
                succHash = genHash(succ);

            dbHelp = new MyDBHelper(getContext());
            SQLiteDatabase db = dbHelp.getWritableDatabase();
            if (pred == null || curr.equals(pred)) {
                //Log.v("Insertion of first query",values.toString());
                Log.v("1","Insert ");
                db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("Insert:", values.toString());

            }
            else if (hashOfKey.compareTo(myPort) > 0 && hashOfKey.compareTo(predHash) > 0 && myPort.compareTo(predHash) < 0){//max key
                Log.v("2", "Insert ");
                db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("Insert:", values.toString());
            }

            else if (hashOfKey.compareTo(myPort) < 0 &&  myPort.compareTo(predHash) < 0){//min key hashOfKey.compareTo(predHash) < 0 &&
                Log.v("3", "Insert ");
                db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("Insert:", values.toString());
            }
            else if (hashOfKey.compareTo(myPort) < 0 && hashOfKey.compareTo(predHash) > 0  && myPort.compareTo(predHash) > 0){
                db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("4", "Insert ");
                Log.v("Insert:", values.toString());
            }
            else if (hashOfKey.compareTo(myPort) > 0 && hashOfKey.compareTo(succHash) < 0 && myPort.compareTo(succHash) < 0){
                //pass it on to succ
                Log.v("5","Insert ");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ)*2);

                    String m = succ + "&&" + key + ",," + value + "&&" + "Insert_New_Key";
                    Log.v("Sending key to succ", m);
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    pw.println(m + "\r\n");
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

                //Log.v("Insert:", values.toString());
            }
            else if (hashOfKey.compareTo(myPort) > 0 && hashOfKey.compareTo(succHash) > 0 && myPort.compareTo(succHash) > 0){
                Log.v("6","Insert ");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ)*2);

                    String m = succ + "&&" + key + ",," + value + "&&" + "Insert_New_Key";
                    Log.v("Sending key to succ", m);
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    pw.println(m + "\r\n");
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(hashOfKey.compareTo(myPort) > 0 && hashOfKey.compareTo(succHash) > 0 && myPort.compareTo(succHash) < 0){
                Log.v("7","Insert ");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ)*2);

                    String m = succ + "&&" + key + ",," + value + "&&" + "Insert_New_Key";
                    Log.v("Sending key to succ", m);
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    pw.println(m + "\r\n");
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(hashOfKey.compareTo(myPort) < 0 && hashOfKey.compareTo(predHash) < 0 && myPort.compareTo(predHash) > 0){
                Log.v("8", "Insert ");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(pred)*2);

                    String m = pred + "&&" + key + ",," + value + "&&" + "Insert_New_Key";
                    Log.v("Sending key to pred", m);
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    pw.println(m + "\r\n");
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
                //1. only 1 node
                //2. newhash<currHash but >succHash //
                //3. newhash>currHash & predHash & predHash>currHash

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        try {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);//5554
            //port = String.valueOf((Integer.parseInt(portStr) * 2));//11108
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(NullPointerException e1){
            e1.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }


        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join" );

        dbHelp = new MyDBHelper(getContext());
        SQLiteDatabase db = dbHelp.getWritableDatabase();
        return false;
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("11108"));
                //Log.v("msgs[0] is",msgs[0]);
                String m = "5554" + "&&" + portStr + "&&" + msgs[0];
                //Log.v("Sending msg from client task", m);
                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                pw.println(m + "\r\n");
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Log.v("Total nodes active"," ");
            if(serverSocket != null) {
                try {
                    while (true) {//m:toport + from port + join + portnumber(myport?)
                        //Log.v("In server of "+portStr, genHash(portStr));

                        Socket clientSocket = serverSocket.accept();
                        BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        String s = br.readLine();
                        s = s.trim();
                        String parts[] = s.split("&&");//splitting the port & msg from s
                        //Log.v("Received from", parts[1]);
                        if (parts[2].equals("Join")) {
                            //Log.v("Join of", parts[1]);
                            if (pred == null)//5554 not active
                            {
                                //if sender/joiner is 5554
                                if (parts[1].equals("5554")) {
                                    pred = succ = curr = parts[0];
                                    //Log.v("Pred is null", "First node");
                                }
                            }
                                //if joiner is else
                            else if(pred.equals("5554") && curr.equals("5554")){
                                pred = succ = parts[1];
                                curr = "5554";
                                //Log.v("Pred updated to", pred);
                                //Log.v("Succ updated to", succ);
                                //Log.v(parts[1], "Second node");
                                //Log.v("genHash of this node is", genHash(parts[1]));
                                //update pred & succ of new node
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(parts[1])*2);

                                    String m = parts[1] + "&&" + portStr + "&&" + "Update_Pred";
                                    //Log.v("Sending msg to 2nd node", m);
                                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                    pw.println(m + "\r\n");
                                    pw.flush();
                                    socket.close();

                                } catch (UnknownHostException e) {
                                    Log.e(TAG, "ClientTask UnknownHostException");
                                } catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException");
                                }
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(parts[1])*2);

                                    String m = parts[1] + "&&" + portStr + "&&" + "Update_Succ";
                                    //Log.v("Sending msg to 2nd node", m);
                                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                    pw.println(m + "\r\n");
                                    pw.flush();
                                    socket.close();

                                } catch (UnknownHostException e) {
                                    Log.e(TAG, "ClientTask UnknownHostException");
                                } catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException");
                                }
                            }
                            else {
                                //Log.v("Hello Sweetie!",parts[1]);
                                //new hashvalue greater than 5554 but smaller than succ
                                String newHash = genHash(parts[1]);
                                myPort = genHash(parts[0]);
                                String succHash = genHash(succ);
                                String predHash = genHash(pred);

                                if (newHash.compareTo(myPort) > 0 && newHash.compareTo(succHash) < 0) {

                                    //Log.v("Case 1","newHash b/w myport & succport");
                                    String old_succ = succ;
                                    //send to succ to update pred
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(succ)*2);

                                        String m = succ + "&&" + parts[1] + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    succ = parts[1];
                                    //Log.v("Succ updated to", succ);
                                    //update pred & succ of new node itself
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(succ)*2);

                                        String m = succ + "&&" + portStr + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(succ)*2);

                                        String m = succ + "&&" + old_succ + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to client", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }

                                } else if (newHash.compareTo(myPort) > 0 && newHash.compareTo(succHash) > 0 && myPort.compareTo(succHash) <= 0) {
                                        //pass over whole node to succ
                                    //Log.v("Case 2","newHash greater than myport & succPort");
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(succ)*2);
                                        //Log.v("Succ of this", succ);
                                        String m = succ + "&&" + parts[1] + "&&" + "Join";
                                        //Log.v("Send join msg to succ", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                }
                                //else if (newHash.compareTo(myPort) > 0 && newHash.compareTo(succPort) > 0 && myPort.compareTo(succPort) > 0) {
                                else if (newHash.compareTo(myPort) > 0 && newHash.compareTo(succHash) > 0 && myPort.compareTo(succHash) > 0) {

                                    //Log.v("Succ updated to", succ);
                                    //updating pred of my succ (so send update_succ to pred)
                                    //Log.v("Updating succ of my pred",myPort);
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(succ)*2);

                                        String m = succ + "&&" + parts[1] + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    //
                                    //update pred & succ of this new node (pred will be old pred)
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + portStr + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + succ + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to client/////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    //update my pred
                                    //pred = parts[1];
                                    succ = parts[1];
                                    //Log.v("Succ updated to", succ);

                                }
                                else if(newHash.compareTo(myPort) < 0 && (predHash.compareTo(myPort) > 0 || newHash.compareTo(predHash) > 0)){
                                   // Log.v("Inside the less than cond", " ");
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(pred)*2);

                                        String m = pred + "&&" + parts[1] + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to pred", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    //
                                    //update pred & succ of this new node (pred will be old pred)
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + pred + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + portStr + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to client/////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                pred = parts[1];
                                    //Log.v("Pred updated to", pred);
                                }
                                else if (newHash.compareTo(myPort) < 0 && newHash.compareTo(predHash) < 0 && predHash.compareTo(myPort)< 0){
                                    //Log.v("Moving node to lesser","newHash lesser than myport & predHash");
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(pred)*2);
                                        //Log.v("Succ of this", succ);
                                        String m = pred + "&&" + parts[1] + "&&" + "Join";
                                        //Log.v("Send join msg to succ", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                }
                                else if(newHash.compareTo(myPort) < 0 && newHash.compareTo(predHash) < 0 && predHash.compareTo(myPort) > 0){
                                    //Log.v("Inside 2nd less than cond", " ");
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(pred)*2);

                                        String m = pred + "&&" + parts[1] + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to pred", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    //
                                    //update pred & succ of this new node (pred will be old pred)
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + pred + "&&" + "Update_Pred";
                                        //Log.v("Sending update pred msg to client////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(parts[1])*2);

                                        String m = parts[1] + "&&" + portStr + "&&" + "Update_Succ";
                                        //Log.v("Sending update succ msg to client/////", m);
                                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                        pw.println(m + "\r\n");
                                        socket.close();

                                    } catch (UnknownHostException e) {
                                        Log.e(TAG, "ClientTask UnknownHostException");
                                    } catch (IOException e) {
                                        Log.e(TAG, "ClientTask socket IOException");
                                    }
                                    pred = parts[1];;
                                    //Log.v("Pred updated to", pred);
                                }

                                //new node greater than 5554 & succ but smaller than pred
                                //new node greater than all three


                            }
                        } else if (parts[2].equals("Update_Pred")) {
                            //Log.v("In client server task pred", portStr);
                            pred = parts[1];
                            curr = parts[0];
                            //succ = parts[1];
                           // Log.v("Pred updated to", pred);

                        } else if (parts[2].equals("Update_Succ")) {
                            //Log.v("In client server task succ", portStr);
                            succ = parts[1];
                            curr = parts[0];
                            //Log.v("Succ updated to", succ);

                        } else if(parts[2].equals("Insert_New_Key")) {
                            ContentValues cv = new ContentValues();
                            String keys[] = parts[1].split(",,");
                           // Log.v("!!!!keys "+keys[0],keys[1]);
                            cv.put("key",keys[0]);
                            cv.put("value", keys[1]);
                            insert(null, cv);
                        }
                        else if(parts[2].equals("Query_Key")) {

                            SQLiteDatabase db = dbHelp.getReadableDatabase();
                            Log.v("In main query"," ");
                            Cursor c = null;
                            c = query(myUri, null, parts[1], null, null, null);
                            //Log.v("Lets check cursor"," ");
                            //Log.v("Returning m", Integer.toString(c.getCount()));
                            c.moveToFirst();
                            String qkey = c.getString(c.getColumnIndex("key"));
                            String qvalue = c.getString(c.getColumnIndex("value"));
                            Log.v(qkey,qvalue);
                            String m = pred + "&&" + qkey + ",," + qvalue + "&&" + "Query_Key";
                            //Log.v("Returning m", m);
                            //Log.v("Sending update pred msg to client", m);
                            PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //Log.v("Returning query", " ");

                        }
                        else if(parts[2].equals("Query_All")){
                            //Log.v("Query all"," ");
                            Log.v(parts[0],pred);
                            Log.v("z",Integer.toString(z));
                            if(z!=1) {
                                Log.v("Msg I got",s);
                                Cursor c = query(myUri, null, parts[1], null, null, null);
                                Log.v("Lets check cursor", " ");
                                String qkey, qvalue;
                                Log.v("Returning m", Integer.toString(c.getCount()));
                                c.moveToFirst();
                                ArrayList all = new ArrayList<String>();
                                while (!c.isAfterLast()) {
                                    Log.v("Inside","while");
                                    qkey = c.getString(c.getColumnIndex("key"));
                                    qvalue = c.getString(c.getColumnIndex("value"));
                                    Log.v(qkey, qvalue);
                                    //Log.v("Sending update pred msg to client", m);

                                    String m = qkey + "&&" + qvalue;
                                    Log.v("Returning m", m);

                                    all.add(m);
                                    //Log.v("Returning query", " ");
                                    c.moveToNext();
                                }
                                String n = all.toString();
                                Log.v("n", n);
                                PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), false);
                                pw.println(n + "\r\n");
                                pw.flush();
                                z=0;
                            }
                            else {
                                Log.v("Coming back to ",parts[0]);
                                String m =null;
                                PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                                pw.println(m + "\r\n");
                                //return null;
                            }
                        }

                        //Log.v("Pred of this", pred);
                        //Log.v("Succ of this", succ);
                        //clientSocket.close();
                    }

                } catch (Exception e) {
                    Log.e(TAG, "Error in server passing messages");
                }
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */


            return;
        }
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        try {
            z=0;
            Log.v("Query Blah"," ");
            Cursor c1;
            MatrixCursor c = new MatrixCursor(new String[]{KEY,VALUE});

            if(succ!=null)
                succHash = genHash(succ);
            if(pred!=null)
                predHash = genHash(pred);

            myPort = genHash(portStr);
            SQLiteDatabase db = dbHelp.getReadableDatabase();

            if ((selection).equals("@")) {
                Cursor cursor = db.rawQuery("SELECT * FROM " + TABLE_NAME, null);
                Log.v("Query:", selection);
                return cursor;
            }
            else if ((selection).equals("*")) {
                Cursor cursor = db.rawQuery("SELECT * FROM " + TABLE_NAME, null);
                Log.v("Cursor count of "+portStr,Integer.toString(cursor.getCount()));
                //Log.v("succ",succ);
                if( succ!=null && pred!=null) {
                    if (!(succ == curr)) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ) * 2);
                            z++;
                            String m = portStr + "&&" + selection + "&&" + "Query_All" ;
                            Log.v("Sending key to succ in *", m);
                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //read
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String s = br.readLine();
                            Log.v("stuff b4 null"," ");

                            if(s.equals("null") || s.equals("[]")) {

                                Log.v("Cursor count of "+portStr,Integer.toString(cursor.getCount()));
                                return cursor;
                            }
                            Log.v("s", s);
                            s=s.substring(1, s.length() - 1);
                            Log.v("s", s);
                            String[] parts = s.split(", ");

                            for( int i=0;i< parts.length;i++) {
                                String[] keys = parts[i].split("&&");
                                Log.v("Received key", keys[0]);
                                c.addRow(new Object[]{keys[0], keys[1]});
                            }
                            socket.close();
                            MergeCursor mC = new MergeCursor(new Cursor[]{cursor, c});
                            Log.v("mCount",Integer.toString(mC.getCount()));
                            z=0;
                            return mC;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                    }

                }

                return cursor;
            }
            if(pred == curr){
                Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                //Log.v("!!!Query:", selection);
                return cursor;
            }
            else if(predHash!=null && succHash!=null) {
                Log.v("pred & succ not null"," ");
                Log.v("Blah 1", selection);
                Log.v("Hash of key",genHash(selection));
                Log.v("Hash of succ",succHash);
                Log.v("Hash of pred",predHash);
                if (genHash(selection).compareTo(myPort) >= 0) {

                    if (genHash(selection).compareTo(succHash) < 0) {
                        Log.v("Blah 2", selection);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ) * 2);

                            String m = succ + "&&" + selection + "&&" + "Query_Key";
                            Log.v("Sending key to succ", m);
                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //read
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String s = br.readLine();
                            String parts[] = s.split("&&");
                            String keys[] = parts[1].split(",,");
                            c.addRow(new Object[]{keys[0], keys[1]});
                            c1 = c;

                            socket.close();
                            return c1;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                        //return cursor;
                    }else if(genHash(selection).compareTo(predHash) > 0 && myPort.compareTo(predHash) < 0 ){
                        Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                        Log.v("!!!Query:", selection);
                        return cursor;
                    }

                    else if (genHash(selection).compareTo(succHash) >= 0 && succHash.compareTo(myPort) < 0){
                        Log.v("Blah case","cabone");
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ) * 2);

                            String m = succ + "&&" + selection + "&&" + "Query_Key";
                            Log.v("Sending key to succ", m);
                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //read
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String s = br.readLine();
                            String parts[] = s.split("&&");
                            String keys[] = parts[1].split(",,");
                            c.addRow(new Object[]{keys[0], keys[1]});
                            c1 = c;

                            socket.close();
                            return c1;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                    }
                    else if (genHash(selection).compareTo(succHash) >= 0 && succHash.compareTo(myPort) > 0) {
                        //send to succ
                        Log.v("Blah 3", selection);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succ) * 2);

                            String m = succ + "&&" + selection + "&&" + "Query_Key";
                            Log.v("Sending key to succ", m);
                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //read
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String s = br.readLine();
                            String parts[] = s.split("&&");
                            String keys[] = parts[1].split(",,");
                            c.addRow(new Object[]{keys[0], keys[1]});
                            c1 = c;

                            socket.close();
                            return c1;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                    }


            /*if(predHash!=null) {

                if (genHash(selection).compareTo(predHash) < 0 && myPort.compareTo(predHash) < 0) {
                    Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                    Log.v("!!!Query:", selection);
                    return cursor;
                }
            }*/
                else {
                    Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                    Log.v("!!!Query:", selection);
                    return cursor;
                }
            } else if (genHash(selection).compareTo(myPort) <= 0) {
                    Log.v("Key less than me", " ");
                    if (genHash(selection).compareTo(predHash) > 0 && myPort.compareTo(predHash) > 0) {
                        Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                        Log.v("!!!Query:", selection);
                        return cursor;


                    } else if (genHash(selection).compareTo(predHash) < 0 && myPort.compareTo(predHash) > 0) {
                        Log.v("Blah","less than case");
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(pred) * 2);

                            String m = pred + "&&" + selection + "&&" + "Query_Key";
                            Log.v("Sending key to pred", m);
                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                            pw.println(m + "\r\n");
                            //read
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String s = br.readLine();
                            String parts[] = s.split("&&");
                            String keys[] = parts[1].split(",,");
                            c.addRow(new Object[]{keys[0], keys[1]});

                            socket.close();
                            c1 = c;
                            return c1;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                    }
                    else if (genHash(selection).compareTo(predHash) < 0 && myPort.compareTo(predHash) < 0) {
                        Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
                        Log.v("!!!Query:", selection);
                        return cursor;


                    }

                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
