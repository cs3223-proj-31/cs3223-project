package simpledb.query;

public class Operator {
    private String op;
    public Operator(String s){
        if(s.equals("<") || s.equals("<=") || s.equals(">") || s.equals(">=") || s.equals("!=") || s.equals("<>") || s.equals("=")) {
            op = s;
        }
        else {
            System.out.println("WARNING: s is not an operator. It is:" + s +".");
        }
    }

    public boolean Compare(Constant a, Constant b) {
        int res = a.compareTo(b);
        if(op.equals(">")) {
            return res > 0;
        }
        if(op.equals(">=")){
            return res >= 0;
        }
        if(op.equals("=")) {
            return res == 0;
        }
        if(op.equals("<=")){
            return res <= 0;
        }
        if(op.equals("<")){
            return res < 0;
        }
        if(op.equals("!=") || op.equals("<>")){
            return res != 0;
        }
        System.out.println("WARNING: operator isnt valid. It is: " + op);
        return false;
    }

    @Override
    public String toString() {
        return op;
    }
}
