package hadoop;

public class TestGenerator {

    public static void main(String[] args) {
        String[] source = {"book12", "book34", "cd12",
                "cd42", "dvd32", "book32", "book34", "dvd32", "cd54", "book11", "cd67"};
        for (int i = 0; i < 50; i++) {
            int count = (int) (11 * Math.random());
            StringBuffer result = new StringBuffer();
            int[] index = randomCommon(0, 10, count);
            for (int j = 0; j < index.length; j++) {
                result.append(source[index[j]]).append(", ");
            }
            System.out.println(result.toString());
        }
    }

    public static int[] randomCommon(int min, int max, int n){
        if (n > (max - min + 1) || max < min) {
            return null;
        }
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            for (int j = 0; j < n; j++) {
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }
}
