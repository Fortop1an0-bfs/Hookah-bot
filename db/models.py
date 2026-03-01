from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from db.database import Base


class Tobacco(Base):
    __tablename__ = "tobaccos"
    __table_args__ = {"comment": "Все известные табаки"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    brand = Column(String(100), nullable=False, comment="Бренд: MustHave, Jent, Darkside")
    flavor = Column(String(200), nullable=False, comment="Вкус: Berry Holls, follar")
    full_name = Column(String(300), nullable=False, comment="Бренд + вкус")
    in_stock = Column(Boolean, default=None, nullable=True,
                      comment="Есть хоть одна граммовка на Металлургической д1")
    # JSON: [{"grams": "25 гр", "url": "...", "in_stock": true}, ...]
    variants = Column(JSON, nullable=True,
                      comment="Все граммовки с URL и наличием. JSON массив")
    origin_date = Column(DateTime(timezone=True), server_default=func.now(),
                         comment="Дата первого появления табака")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(),
                        onupdate=func.now(),
                        comment="Дата последнего обновления (проверка наличия)")

    mix_entries = relationship("MixTobacco", back_populates="tobacco")


class Mix(Base):
    __tablename__ = "mixes"
    __table_args__ = {"comment": "Миксы из Telegram"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(36), unique=True,
                  default=lambda: str(uuid.uuid4())[:8].upper(),
                  comment="Короткий код: A1B2C3D4")
    original_text = Column(Text, nullable=True, comment="Оригинальный текст сообщения")
    source_channel = Column(String(200), nullable=True, comment="Источник (канал/пользователь)")
    is_available = Column(Boolean, default=None, nullable=True,
                          comment="True=все есть, False=чего-то нет, NULL=не проверялось")
    missing_tobaccos = Column(Text, nullable=True,
                              comment="Табаки которых нет, через запятую")
    # Краткое описание состава для отображения: "MustHave Ваниль 30%, Груша 60%"
    tobaccos_summary = Column(Text, nullable=True,
                              comment="Краткий состав микса для быстрого просмотра")
    title = Column(String(200), nullable=True,
                   comment="Пользовательское название микса, заданное вручную")
    origin_date = Column(DateTime(timezone=True), server_default=func.now(),
                         comment="Дата первого сохранения")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(),
                        onupdate=func.now(),
                        comment="Дата последнего обновления наличия")

    tobaccos = relationship("MixTobacco", back_populates="mix",
                            order_by="MixTobacco.mix_id")


class MixTobacco(Base):
    __tablename__ = "mix_tobaccos"
    __table_args__ = {"comment": "Состав миксов"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    mix_id = Column(Integer, ForeignKey("mixes.id"), nullable=False,
                    comment="Ссылка на микс", index=True)
    tobacco_id = Column(Integer, ForeignKey("tobaccos.id"), nullable=True,
                        comment="Ссылка на табак (NULL если не найден)")
    raw_brand = Column(String(100), nullable=False, comment="Бренд как в оригинале")
    raw_flavor = Column(String(200), nullable=False, comment="Вкус как в оригинале")
    percentage = Column(Float, nullable=True, comment="Процент в миксе")

    mix = relationship("Mix", back_populates="tobaccos")
    tobacco = relationship("Tobacco", back_populates="mix_entries")
