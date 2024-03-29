package simpledb.parse;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LexerTest2 {

	@Test
	void queryEquality_success() {
		String x; String y; int z;
		String query = "a = 1";
		Lexer lex = new Lexer(query);

		x = lex.eatId();
		assertEquals("a", x);

		y = lex.eatOp();
		assertEquals("=", y);

		z = lex.eatIntConstant();
		assertEquals(1, z);
	}
	
	

}
