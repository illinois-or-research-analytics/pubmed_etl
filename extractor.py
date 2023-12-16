def main(gz, cores):
    files = os.listdir(gz)
    gz_files = [file for file in files if '.gz' in file]

    counter = 0

    while len(gz_files) > 0 and counter < 50:
        counter += 1
        lst = []

        for f in gz_files:
            lst.append((f,gz))


        with multiprocessing.Pool(processes=cores) as pool:
            results = pool.starmap(extractor, lst)

        files = os.listdir(gz)
        gz_files = [file for file in files if '.gz' in file]






def extractor(filename, base_dir):
    os.system(f'cd {base_dir} && gunzip {filename}')


if __name__ == '__main__':
    import os
    import multiprocessing
    import argparse

    parser = argparse.ArgumentParser(description='Parser')
    parser.add_argument('-gz', dest='gz', type=str, help='Path to gz files directory')
    parser.add_argument('-cores', dest='cores', type=str, help='Int, num cores')

    args = parser.parse_args()
    gz = args.gz
    cores = int(args.cores)

    if gz[-1] != '/':
        gz += '/'

    main(gz, cores)

