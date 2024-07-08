FROM python:3.7
ENV PYTHONUNBUFFERED 1
RUN mkdir -p /data/goin/goin_back_data_parse_writer_web
WORKDIR /data/goin/goin_back_data_parse_writer_web
COPY requirement.txt /data/goin/goin_back_data_parse_writer_web

# docker镜像构造这里先不让代码直接在容器里.等docker run的时候在主机做映射.
# ADD . /usr/etl-graph-converter/app
# 更改默认apt源
RUN  sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list
RUN  apt-get clean

# 让容器下载 delete tab补全之类的常规功能
# RUN apt-get install bash-completion -y -q &&bash
# 替换国内pip源
RUN pip install pip -U
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
# 下载项目依赖
RUN pip3 install  --default-timeout=1000 -r requirement.txt
