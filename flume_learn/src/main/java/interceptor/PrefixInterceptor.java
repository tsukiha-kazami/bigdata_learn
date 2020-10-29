package interceptor;

import com.google.common.base.Charsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * @author Shi Lei
 * @create 2020-10-28
 */
public class PrefixInterceptor implements Interceptor {

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {
    if (event == null) {
      return null;
    }

    String line = new String(event.getBody(), Charsets.UTF_8);

    String newLine = "Shi Lei ".concat(line);
    event.setBody(newLine.getBytes(Charsets.UTF_8));
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> out = new ArrayList<Event>();
    for (Event event : events) {
      Event outEvent = intercept(event);
      if (outEvent != null) {
        out.add(outEvent);
      }
    }
    return out;
  }

  @Override
  public void close() {

  }


  public static class MyBuilder implements Interceptor.Builder {

    public void configure(Context context) {
    }

    /*
     * @see org.apache.flume.interceptor.Interceptor.Builder#build()
     */
    public PrefixInterceptor build() {
      return new PrefixInterceptor();
    }
  }
}
