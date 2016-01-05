import subprocess
import os, sys, time, signal


tests = ['ddl.txt',
		'test1.txt', 
		'test2.txt', 
		'test3.txt', 
		'test4.txt', 
		'test5.txt', 
		'test6.txt', 
		'test7.txt',
		'test08.dsl',
		'test09.dsl',
		'test10.dsl',
		'test11.dsl',
		'test12.dsl',
		'test13.dsl',
		'test14.dsl',
		'test15.dsl',
		'test16.dsl',
		'test17.dsl',
		'test18.dsl',
		'test19.dsl',
		'test20.dsl',
		'test21.dsl',
		'test22.dsl',
		'test23.dsl',
		'test24.dsl',
		'test25.dsl',
		'test26.dsl',
		'test27.dsl',
		'test28.dsl',
		'test29.dsl',
		'test30.dsl',
		'test31.dsl',
		'test32.dsl',
		'test33.dsl']


expected_results = [None,
					'test1_exp.txt',
					'test2_exp.txt',
					'test3_exp.txt',
					'test4_exp.txt',
					'test5_exp.txt',
					'test6_exp.txt',
					'test7_exp.txt',
					None,
					None,
					'test10.exp',
					'test11.exp',
					'test12.exp',
					'test13.exp',
					None,
					None,
					'test16.exp',
					'test17.exp',
					'test18.exp',
					'test19.exp',
					'test20.exp',
					'test21.exp',
					'test22.exp',
					'test23.exp',
					'test24.exp',
					None,
					'test26.exp',
					'test27.exp',
					'test28.exp',
					None,
					'test30.exp',
					'test31.exp',
					'test32.exp',
					'test33.exp']

tests_path_str = '../project_tests/'
sort_str = ' | sort > out.txt'

def check_line(out, expected):
	if '.' not in out:
		return out == expected
	else:
		out_ints = [int(float(x)) for x in out.split(',')]
		expected_ints = [int(float(x)) for x in expected.split(',')]
		return out_ints == expected_ints

def is_server_open(pid):
    try:
        os.kill(pid, 0)
    except OSError:
    	print "bad"
        return False
    else:
    	print "good"
        return True
	print "meh"
	return False


def verify(out_file, expected_file):
	with open(out_file) as out:
		with open(expected_file) as expected:
			for expected_line in expected:
				out_line = out.readline()
				if not check_line(out_line, expected_line):
					return False
			last_out_line = out.readline()
			if last_out_line != "":
				return False
	return True



def main():
	relaunch = True
	server_pid = 0
	ignore=open('/dev/null')
	for i, test in enumerate(tests):
		print "Test", i, ": ",
		if relaunch:
			try:
				server_pid = subprocess.Popen(['./server'], stdout=ignore).pid
				relaunch = False;
			except:
				print "Failed to launch server, batch aborted"
				sys.exit(1)

		try:
			test_p = subprocess.check_output('./client < ../project_tests/' + test + ' | sort > out.txt', shell=True)
		except Exception, err:
			print Exception, err, 'oo'
			sys.exit(1)

		if expected_results[i] != None:
			try:
				z = subprocess.check_output('sort ../project_tests/' + expected_results[i] + ' > expected.txt', shell=True)
			except:
				print "bad"
				sys.exit(1)
			if verify('out.txt', 'expected.txt'):
				print "PASSED"
			else:
				print "FAILED"
				exit(1)
		else:
			print "PASSED"
		if 'shutdown' in open('../project_tests/' + test).read():
			time.sleep(8)
			relaunch = True
	
	os.kill(server_pid, signal.SIGTERM)

if __name__ == '__main__':
	main()
