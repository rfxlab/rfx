package rfx.core.util.math;

import rx.Observable;
import rx.Observer;
import rx.observables.MathObservable;

public class TestRxMath {

	public static void main(String[] args) {		
		Observer<Float> w = new Observer<Float>() {
			
			@Override
			public void onNext(Float t) {
				System.out.println(t);
			}
			
			@Override
			public void onError(Throwable e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onCompleted() {
				// TODO Auto-generated method stub
				
			}
		};
		Observable<Float> src = Observable.just(1f, 2f, 3f, 4f);
		MathObservable.averageFloat(src).subscribe(w);
		
		
	}
}
