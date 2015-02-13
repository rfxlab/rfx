package rfx.core.util.test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Test;

public class TestStringUtil {

	@Test
	public void test1(){
		// mock creation
		List<String> mockedList = mock(List.class);

		// using mock object - it does not throw any "unexpected interaction" exception
		mockedList.add("one");
		mockedList.clear();

		// selective, explicit, highly readable verification
		verify(mockedList).add("one");		
		verify(mockedList).clear();
	}
}
